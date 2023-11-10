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
export function runQuery(dbase: Database, sql: string, params: Array<number | string | boolean | number[]>) {
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
export async function getMeasurementsFromDBs(fromDate: dayjs.Dayjs, toDate: dayjs.Dayjs, ip: string, channel?: number | number[]): Promise<Measurement[]> {
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
                let filters: (string | number | number[])[] = [fromSec, toSec];
                let placeholders = "";
                if (channel) {
                    if (Array.isArray(channel)) {
                        placeholders = channel.join(",");
                    } else {
                        filters.push(channel);
                    }
                }
                //console.log(dayjs().format(), "filters:", filters);
                const sql = "select * from measurements where recorded_time between ? and ? "
                    + (channel ? (Array.isArray(channel) ? `and channel in (${placeholders})` : "and channel=?") : "") + " order by recorded_time, channel";
                //console.log(dayjs().format(), "sql:", sql);
                let measurements = await runQuery(db, sql, filters);
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
 * @param powermeterTimeZone time zone of server
 * @param details hourly|daily|monthly details 
 * @param isAggregation add the first record of measured value to result (process is aggregation)
 * @returns the detailed measurements
 */
export function getDetails(measurements: Measurement[], powermeterTimeZone: string, details: string, isAggregation: boolean) {
    let result: RecElement[] = [];
    let prevElement: RecElement[] = [];
    let lastElement: RecElement[] = [];

    const isHourlyEnabled = details == 'hourly';
    const isDaily = details == 'daily';
    const isMonthly = details == 'monthly';

    let isAddableEntry = false;

    const localTimeZone = dayjs.tz.guess();
    //dayjs.tz.setDefault(timeZone);

    let isDailyEnabled: boolean = false;
    let isMonthlyEnabled: boolean = false;

    let prevRecTime: number = 0;

    measurements.forEach((element: Measurement, idx: number) => {
        if (prevElement[element.channel] == undefined) {
            initPrevElement(element, prevElement);
            prevRecTime = element.recorded_time;
            if (isAggregation) {
                result.push({ ...prevElement[element.channel] });
            }
        } else {
            const isChangedTime = prevRecTime !== element.recorded_time;
            if (isChangedTime) {
                isDailyEnabled = isDaily && checkDaylyEnabled(element, prevElement);
                isMonthlyEnabled = isMonthly && checkMonthlyEnabled(element, prevElement);
            }
            //fileLog("measurements.log", `${dayjs.unix(element.recorded_time).tz().format("YYYY-MM-DD HH:mm:ss")} | diff: ${diffMonths}\n`);
            isAddableEntry = isHourlyEnabled || isDailyEnabled || isMonthlyEnabled;

            if (isAddableEntry) {
                calcDiff(element, prevElement, powermeterTimeZone, localTimeZone);
                result.push({ ...prevElement[element.channel] });
            }
            prevRecTime = element.recorded_time;
            lastElement[element.channel] = { recorded_time: element.recorded_time, measured_value: element.measured_value, channel: element.channel };
        }
    });
    if (!isAddableEntry && lastElement.length > 0) {
        appendLastElement(result, prevElement, lastElement, powermeterTimeZone, localTimeZone);
    }
    return result;
}

interface ResultAVG {
    channel: number,
    sum: number,
    avg: number,
    count: number
}

export function roundToFourDecimals(value: number) {
    return Math.round(value * 10000) / 10000;
}

/**
 * Get the summ of measurements
 * @param measurements array of measurements
 * @param powermeterTimeZone time zone of powermeter
 * @returns the array of summ values
 */
export function getAvgSum(measurements: Measurement[], powermeterTimeZone: string) {
    const details = getDetails(measurements, powermeterTimeZone, "hourly", false);
    const result: ResultAVG[] = [];
    details.forEach((element: RecElement, idx: number) => {
        const item = result.find(value => value.channel === element.channel);
        if (!item) {
            if (element.diff) {
                let value: ResultAVG = { channel: element.channel, sum: element.diff, avg: 0, count: 1 };
                result.push(value);
            }
        } else {
            if (element.diff) {
                if (item) {
                    item.sum += element.diff;
                    item.count += 1;
                }
            }
        }
    });
    result.forEach((element: ResultAVG, idx: number) => {
        element.sum = roundToFourDecimals(element.sum);
        element.avg = roundToFourDecimals(element.sum / element.count);
    });
    return result;
}

/**
 * Get monthly measurements from previous year
 * 
 * @param fromDate from date
 * @param toDate to date
 * @param ip IP address of powermeter
 * @param channel channel of powermeter (use -1 for all)
 * @returns the array of measurements
 */
export async function getYearlyMeasurementsFromDBs(fromDate: dayjs.Dayjs, toDate: dayjs.Dayjs, ip: string, channel?: number | number[]): Promise<any[]> {
    let result: any[] = [];
    const filePath = (process.env.WORKDIR as string);
    const dbFile = path.join(filePath, ip, fromDate.format("YYYY") + "-yearly.sqlite");
    if (fs.existsSync(dbFile)) {
        const db = new Database(dbFile);
        try {
            const fromSec = fromDate.unix();
            const toSec = toDate.unix();
            let filters = [fromSec, toSec];
            let placeholders = "";
            if (channel) {
                if (Array.isArray(channel)) {
                    placeholders = channel.join(",");
                } else {
                    filters.push(channel);
                }
            }
            let measurements = await runQuery(db, "select * from measurements where recorded_time between ? and ? " + (channel ? (Array.isArray(channel) ? `and channel in (${placeholders})` : "and channel=?") : "") + " order by recorded_time, channel", filters);
            measurements.forEach((element: any) => {
                result.push(element);
            })
        } catch (err) {
            console.error(dayjs().format(), err);
        } finally {
            db.close();
        }
    }

    return result;
}

/**
 * Calculate the difference between two measurements
 * 
 * @param element measurement element
 * @param prevElement previous measurement elements
 * @param powermeterTimeZone server time zone
 * @param localTimeZone local time zone
 */
function calcDiff(element: RecElement, prevElement: RecElement[], powermeterTimeZone: string, localTimeZone: string) {
    const diff = roundToFourDecimals(element.measured_value - prevElement[element.channel].measured_value);
    prevElement[element.channel] = {
        recorded_time: element.recorded_time,

        from_utc_time: dayjs.unix(prevElement[element.channel].recorded_time).utc().format("YYYY-MM-DD HH:mm:ss"),
        to_utc_time: dayjs.unix(element.recorded_time).utc().format("YYYY-MM-DD HH:mm:ss"),

        from_unix_time: dayjs.unix(prevElement[element.channel].recorded_time).format("YYYY-MM-DD HH:mm:ss"),
        to_unix_time: dayjs.unix(element.recorded_time).format("YYYY-MM-DD HH:mm:ss"),

        from_powermeter_time: dayjs.unix(prevElement[element.channel].recorded_time).tz(powermeterTimeZone).format("YYYY-MM-DD HH:mm:ss"),
        to_powermeter_time: dayjs.unix(element.recorded_time).tz(powermeterTimeZone).format("YYYY-MM-DD HH:mm:ss"),

        from_local_time: dayjs.unix(prevElement[element.channel].recorded_time).tz(localTimeZone).format("YYYY-MM-DD HH:mm:ss"),
        to_local_time: dayjs.unix(element.recorded_time).tz(localTimeZone).format("YYYY-MM-DD HH:mm:ss"),

        measured_value: element.measured_value,
        channel: element.channel,
        diff: diff
    };
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

/**
 * Check if dayly details is enabled
 * 
 * @param element Measurement element
 * @param prevElement Previous measurement elements
 * @returns true if the difference between the two measurement is greater than or equale to 1 day
 */
function checkDaylyEnabled(element: Measurement, prevElement: RecElement[]): boolean {
    let roundedPrevDay = dayjs.unix(prevElement[element.channel].recorded_time).set("hour", 0).set("minute", 0).set("second", 0); //.tz()
    let roundedDay = dayjs.unix(element.recorded_time).set("hour", 0).set("minute", 0).set("second", 0); //.tz()
    let diffDays = roundedDay.diff(roundedPrevDay, "days");
    return diffDays >= 1;

}

/**
 * Check if monthly details is enabled
 * 
 * @param element Measurement element
 * @param prevElement Previous measurement elements
 * @returns true if the difference between the two measurement is greater than or equale to 1 month
 */
function checkMonthlyEnabled(element: Measurement, prevElement: RecElement[]): boolean {
    let roundedPrevMonth = dayjs.unix(prevElement[element.channel].recorded_time).set("date", 1).set("hour", 0).set("minute", 0).set("second", 0); //.tz()
    let roundedMonth = dayjs.unix(element.recorded_time).set("date", 1).set("hour", 0).set("minute", 0).set("second", 0); //.tz()
    let diffMonths = roundedMonth.diff(roundedPrevMonth, "months", true);
    return diffMonths >= 0.9; //DaylightSavingTime;
}

/**
 * Init previous measurement element
 * 
 * @param element Measurement element
 * @param prevElement Previous measurement elements
 */
function initPrevElement(element: Measurement, prevElement: RecElement[]) {
    prevElement[element.channel] = {
        recorded_time: element.recorded_time,
        measured_value: element.measured_value,
        channel: element.channel,
        diff: 0,
    };
}

/**
 * Append last measurement element to result
 * 
 * @param result the array of measurements
 * @param prevElement previous measurement elements
 * @param lastElement last measurement elements
 * @param serverTimeZone server time zone
 * @param localTimeZone local time zone
 */
function appendLastElement(result: RecElement[], prevElement: RecElement[], lastElement: RecElement[], serverTimeZone: string, localTimeZone: string) {
    lastElement.forEach((element: RecElement, idx: number) => {
        try {
            calcDiff(element, prevElement, serverTimeZone, localTimeZone);
            result.push({ ...prevElement[element.channel] });

        } catch (err) {
            console.error(dayjs().format(), err);
        }
    });
}

/**
 * Get timezone of powermeter
 * 
 * @param ip IP address of powermeter
 * @returns the timezone of powermeter
 */
export async function getPowerMeterTimeZone(ip: string) {
    const configDB = new Database(process.env.CONFIG_DB_FILE as string);

    let timeZone = dayjs.tz.guess();
    const tzone = await runQuery(configDB, "select time_zone from power_meter where ip_address=?", [ip]);
    if (tzone.length > 0) {
        timeZone = tzone[0].time_zone;
    }
    return timeZone;
}