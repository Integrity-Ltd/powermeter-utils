import dayjs, { Dayjs } from "dayjs";
import utc from 'dayjs/plugin/utc'
import timezone from 'dayjs/plugin/timezone'
import { Database } from "sqlite3";
import Net from 'net';
import sqlite3 from 'sqlite3';
import fs from "fs";
import path from "path";
import { Measurement, PowerMeter, RecElement } from "./types";

dayjs.extend(utc)
dayjs.extend(timezone)

/**
 * Run query on database
 * 
 * @param dbase The destination database
 * @param sql SQL command to run
 * @param params params of SQL command
 * @returns Promis for result
 */
export function runQuery(dbase: Database, sql: string, params: Array<number | string | boolean>) {
    return new Promise<any>((resolve, reject) => {
        return dbase.all(sql, params, (err: any, res: any) => {
            if (err) {
                console.error("Run query Error: ", err.message);
                return reject(err.message);
            }
            return resolve(res);
        });
    });
}

/**
 * Insert data to db from powermeter
 * 
 * @param currentTime current timestamp
 * @param powermeter Powermeter data
 * @param channels array of active channel ids
 * @returns 
 */
export async function getMeasurementsFromPowerMeter(currentTime: dayjs.Dayjs, powermeter: PowerMeter, channels: string[]): Promise<boolean | Error> {
    return new Promise<boolean | Error>((resolve, reject) => {
        let response = '';
        const client = new Net.Socket();
        client.setTimeout(5000);
        try {
            client.connect({ port: powermeter.port, host: powermeter.ip_address }, () => {
                console.log(dayjs().format(), powermeter.ip_address, `TCP connection established with the server.`);
                client.write('read all');
            });
        } catch (err) {
            console.error(dayjs().format(), powermeter.ip_address, err);
            reject(err);
        }
        client.on('timeout', function () {
            console.error(dayjs().format(), powermeter.ip_address, "Connection timeout");
            reject(new Error("Connection timeout"));
        });

        client.on('error', function (err) {
            console.error(dayjs().format(), powermeter.ip_address, err);
            client.destroy();
            reject(err);
        });
        client.on('data', function (chunk) {
            response += chunk.toString('utf8');
            if (response.indexOf("channel_13") > 0) {
                client.end();
            }
        });

        client.on('end', async function () {
            console.log(dayjs().format(), powermeter.ip_address, "Data received from the server.");
            let db: Database | undefined = await getMeasurementsDB(powermeter.ip_address, currentTime.format("YYYY-MM") + '-monthly.sqlite', true);
            if (db) {
                try {
                    console.log(dayjs().format(), powermeter.ip_address, "Try lock DB.");
                    await runQuery(db, "BEGIN EXCLUSIVE", []);
                    console.log(dayjs().format(), powermeter.ip_address, "allowed channels:", channels.length);
                    processMeasurements(db, currentTime, powermeter.ip_address, response, channels);
                } catch (err) {
                    console.log(dayjs().format(), powermeter.ip_address, `DB access error: ${err}`);
                    reject(err);
                }
                finally {
                    try {
                        await runQuery(db, "COMMIT", []);
                    } catch (err) {
                        console.log(dayjs().format(), powermeter.ip_address, `Commit transaction error: ${err}`);
                        reject(err)
                    }
                    console.log(dayjs().format(), powermeter.ip_address, 'Closing DB connection...');
                    db.close();
                    console.log(dayjs().format(), powermeter.ip_address, 'DB connection closed.');
                    console.log(dayjs().format(), powermeter.ip_address, 'Closing TCP connection...');
                    client.destroy();
                    console.log(dayjs().format(), powermeter.ip_address, 'TCP connection destroyed.');
                    resolve(true);
                }
            } else {
                console.error(dayjs().format(), powermeter.ip_address, "No database exists.");
                reject(new Error("No database exists."));
            }
        });
    });
}

/**
 * Get measurements from SQLite database
 * @param IPAddress IPv4 address of powermeter
 * @param fileName name of SQLite database file
 * @param create create database file if missing
 * @returns SQLite Database
 */
export async function getMeasurementsDB(IPAddress: string, fileName: string, create: boolean): Promise<Database | undefined> {
    let db: Database | undefined = undefined;
    const dbFilePath = getDBFilePath(IPAddress);
    if (!fs.existsSync(dbFilePath) && create) {
        fs.mkdirSync(dbFilePath, { recursive: true });
        console.log(dayjs().format(), `Directory '${dbFilePath}' created.`);
    }
    const dbFileName = path.join(dbFilePath, fileName);
    if (!fs.existsSync(dbFileName)) {
        if (create) {
            db = new Database(dbFileName, sqlite3.OPEN_READWRITE | sqlite3.OPEN_CREATE);
            console.log(dayjs().format(), `DB file '${dbFileName}' created.`);
            const result = await runQuery(db, `CREATE TABLE "Measurements" ("id" INTEGER NOT NULL,"channel" INTEGER,"measured_value" REAL,"recorded_time" INTEGER, PRIMARY KEY("id" AUTOINCREMENT))`, []);
            console.log(dayjs().format(), "Measurements table created:", result);
        }
    } else {
        console.log(dayjs().format(), `DB file '${dbFileName}' opened.`);
        db = new Database(dbFileName, sqlite3.OPEN_READWRITE);
    }
    return db;
}

/**
 * Process the downloaded data string from powermeter
 * 
 * @param db SQLite database to write measured data
 * @param currentTime current timestamp
 * @param ip_address IPv4 address of powermeter
 * @param response downloaded data string from powermeter
 * @param channels array of active channel ids
 */
export function processMeasurements(db: Database, currentTime: dayjs.Dayjs, ip_address: string, response: string, channels: String[]) {
    let currentTimeRoundedToHour = dayjs(currentTime).set("minute", 0).set("second", 0).set("millisecond", 0);
    console.log(dayjs().format(), ip_address, "currentTimeRoundedToHour:", currentTimeRoundedToHour.format());
    //console.log(dayjs().format(), "received response:", response);
    response.split('\n').forEach((line) => {
        let matches = line.match(/^channel_(\d{1,2}) : (.*)/);
        if (matches && channels.includes(matches[1])) {
            let measuredValue = parseFloat(matches[2]) * 1000;
            db.exec(`INSERT INTO Measurements (channel, measured_value, recorded_time) VALUES (${matches[1]}, ${measuredValue}, ${currentTimeRoundedToHour.unix()})`);
            console.log(dayjs().format(), ip_address, matches[1], matches[2]);
        }
    });
}

/**
 * Read measurements from SQLite
 * 
 * @param fromDate From date of data
 * @param toDate To date of data
 * @param ip IPv4 address of powermeter
 * @param channel filtered channel number
 * @returns promis for array of measurements
 */
export async function getMeasurementsFromDBs(fromDate: dayjs.Dayjs, toDate: dayjs.Dayjs, ip: string, channel?: number): Promise<Measurement[]> {
    let monthlyIterator = dayjs(fromDate).set("date", 1).set("hour", 0).set("minute", 0).set("second", 0).set("millisecond", 0);
    let result: Measurement[] = [];
    while (monthlyIterator.isBefore(toDate) || monthlyIterator.isSame(toDate)) {
        const filePath = (process.env.WORKDIR as string);
        const dbFile = path.join(filePath, ip, monthlyIterator.format("YYYY-MM") + "-monthly.sqlite");
        if (fs.existsSync(dbFile)) {
            const db = new Database(dbFile);
            try {
                const fromSec = fromDate.unix();
                const toSec = toDate.unix();
                let filters = [fromSec, toSec];
                if (channel) {
                    filters.push(channel);
                }
                let measurements = await runQuery(db, "select * from measurements where recorded_time between ? and ? " + (channel ? "and channel=?" : "") + " order by recorded_time, channel", filters);
                result.push(...measurements);
            } catch (err) {
                console.error(dayjs().format(), err);
            } finally {
                db.close();
            }
        }
        monthlyIterator = monthlyIterator.add(1, "months");
    }

    return result;
}

/**
 * Get filtered array of measurements
 * 
 * @param measurements array of measurements
 * @param timeZone time zone of measurements
 * @param details hourly|daily|monthly details 
 * @param addFirst add the first record of measured value to result (process is aggregation)
 * @returns the detailed measurements
 */
export function getDetails(measurements: Measurement[], timeZone: string, details: string, addFirst: boolean) {
    let result: RecElement[] = [];
    let prevElement: RecElement[] = [];
    let lastElement: RecElement[] = [];
    const isHourlyEnabled = details == 'hourly';
    const isDaily = details == 'daily';
    const isMonthly = details == 'monthly';
    let isAddableEntry = false;
    const localTimeZone = dayjs.tz.guess();
    //dayjs.tz.setDefault(timeZone);
    let roundedPrevDay: Dayjs | null = null;
    let roundedDay: Dayjs | null = null;
    let roundedPrevMonth: Dayjs | null = null;
    let roundedMonth: Dayjs | null = null;
    let diffMonths: number = 0;
    let diffDays: number = 0;
    let isDailyEnabled: boolean = false;
    let isMonthlyEnabled: boolean = false;
    let prevRecTime: number = 0;
    measurements.forEach((element: Measurement, idx: number) => {
        if (prevElement[element.channel] == undefined) {
            prevElement[element.channel] = {
                recorded_time: element.recorded_time,
                measured_value: element.measured_value,
                channel: element.channel, diff: 0,
            };

            prevRecTime = element.recorded_time;

            if (addFirst) {
                result.push({ ...prevElement[element.channel] });
            }
        } else {
            const changedTime = prevRecTime !== element.recorded_time;
            if (changedTime) {
                if (isDaily) {
                    roundedPrevDay = dayjs.unix(prevElement[element.channel].recorded_time).set("hour", 0).set("minute", 0).set("second", 0); //.tz()
                    roundedDay = dayjs.unix(element.recorded_time).set("hour", 0).set("minute", 0).set("second", 0); //.tz()
                    diffDays = roundedDay.diff(roundedPrevDay, "days");
                    isDailyEnabled = diffDays >= 1;
                } else {
                    isDailyEnabled = false;
                }
                if (isMonthly) {
                    roundedPrevMonth = dayjs.unix(prevElement[element.channel].recorded_time).set("date", 1).set("hour", 0).set("minute", 0).set("second", 0); //.tz()
                    roundedMonth = dayjs.unix(element.recorded_time).set("date", 1).set("hour", 0).set("minute", 0).set("second", 0); //.tz()
                    diffMonths = roundedMonth.diff(roundedPrevMonth, "months", true);
                    isMonthlyEnabled = diffMonths >= 0.9;
                } else {
                    isMonthlyEnabled = false;
                }
            }
            //fileLog("measurements.log", `${dayjs.unix(element.recorded_time).tz().format("YYYY-MM-DD HH:mm:ss")} | diff: ${diffMonths}\n`);
            isAddableEntry = isHourlyEnabled || isDailyEnabled || isMonthlyEnabled;

            if (isAddableEntry) {
                prevElement[element.channel] = {
                    recorded_time: element.recorded_time,
                    from_utc_time: dayjs.unix(prevElement[element.channel].recorded_time).utc().format("YYYY-MM-DD HH:mm:ss"),
                    to_utc_time: dayjs.unix(element.recorded_time).utc().format("YYYY-MM-DD HH:mm:ss"),
                    from_server_time: dayjs.unix(prevElement[element.channel].recorded_time).tz(timeZone).format("YYYY-MM-DD HH:mm:ss"),
                    to_server_time: dayjs.unix(element.recorded_time).tz(timeZone).format("YYYY-MM-DD HH:mm:ss"),
                    from_local_time: dayjs.unix(prevElement[element.channel].recorded_time).tz(localTimeZone).format("YYYY-MM-DD HH:mm:ss"),
                    to_local_time: dayjs.unix(element.recorded_time).tz(localTimeZone).format("YYYY-MM-DD HH:mm:ss"),
                    measured_value: element.measured_value,
                    channel: element.channel,
                    diff: element.measured_value - prevElement[element.channel].measured_value
                };
                result.push({ ...prevElement[element.channel] });
            }
            prevRecTime = element.recorded_time;
            lastElement[element.channel] = { recorded_time: element.recorded_time, measured_value: element.measured_value, channel: element.channel };
        }
    });
    if (!isAddableEntry && lastElement.length > 0) {
        lastElement.forEach((element: RecElement, idx: number) => {
            try {
                const diff = element.measured_value - prevElement[element.channel].measured_value;
                prevElement[element.channel] = {
                    recorded_time: element.recorded_time,
                    from_utc_time: dayjs.unix(prevElement[element.channel].recorded_time).utc().format("YYYY-MM-DD HH:mm:ss"),
                    to_utc_time: dayjs.unix(element.recorded_time).utc().format("YYYY-MM-DD HH:mm:ss"),
                    from_server_time: dayjs.unix(prevElement[element.channel].recorded_time).tz(timeZone).format("YYYY-MM-DD HH:mm:ss"),
                    to_server_time: dayjs.unix(element.recorded_time).tz(timeZone).format("YYYY-MM-DD HH:mm:ss"),
                    from_local_time: dayjs.unix(prevElement[element.channel].recorded_time).tz(localTimeZone).format("YYYY-MM-DD HH:mm:ss"),
                    to_local_time: dayjs.unix(element.recorded_time).tz(localTimeZone).format("YYYY-MM-DD HH:mm:ss"),
                    measured_value: element.measured_value,
                    channel: element.channel,
                    diff: diff
                };

                result.push({ ...prevElement[element.channel] });

            } catch (err) {
                console.error(dayjs().format(), err);
            }
        });
    }
    return result;
}

/**
 * Get SQLite Database file name
 * 
 * @param IPAddress IPv4 address of powermeter
 * @returns the filename of sqlite database
 */
export function getDBFilePath(IPAddress: string): string {
    const dbFilePath = path.join(process.env.WORKDIR as string, IPAddress);
    return dbFilePath;
}