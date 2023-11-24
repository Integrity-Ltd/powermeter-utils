/**
 * Powermeter records
 * @notExported
 */
export interface PowerMeter {
    id: number;
    asset_name: string;
    ip_address: string;
    port: number;
    time_zone: string;
    enabled: boolean;
}

/**
 * Channel records
 * @notExported
 */
export interface Channel {
    id: number;
    power_meter_id: number;
    channel: number;
    channel_name: string;
    enabled: boolean;
}

/**
 * Measurement records
 * @notExported
 */

export interface Measurement {
    id: number;
    recorded_time: number;
    measured_value: number;
    channel: number;
}

/**
 * Records of measurements
 * @notExported
 */
export interface RecElement {
    recorded_time: number;
    measured_value: number;
    channel: number;
    diff?: number;
    multipliedValue?: number;
    from_utc_time?: stringnumber;
    to_utc_time?: stringnumber;
    from_unix_time?: stringnumber;
    to_unix_time?: stringnumber;
    from_powermeter_time?: stringnumber;
    to_powermeter_time?: stringnumber;
    from_local_time?: stringnumber;
    to_local_time?: stringnumber;
}
