import fs from "fs";

/**
 * Log to file
 * @param fileName name of logfile
 * @param content content of log
 */
export function fileLog(fileName: string, content: string) {
    if (!fs.existsSync(fileName)) {
        fs.writeFileSync(fileName, content)
    } else {
        fs.appendFileSync(fileName, content);
    }
}