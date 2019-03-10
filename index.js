
const Writable = require('stream').Writable;
const ApidbBulkWriter = require('./apidb-bulk-writer');

const DEFAULT_BULK_SIZE = 10000;

class OsmApidbWriter extends Writable {
    constructor(options = {}) {
        super({ objectMode: true });

        this._curBulk = [];
        this._bulkSize = options.bulkSize || DEFAULT_BULK_SIZE;
        this._apidbWriter = new ApidbBulkWriter(options.pgConnectionProperties);
    }

    async _write(chunk, encoding, callback) {
        try {
            this._curBulk.push(chunk);
            
            if (this._curBulk.length === this._bulkSize) {
                if (!this._apidbWriter.inited) {
                    await this._apidbWriter.initWrite();
                }

                await this._apidbWriter.writeEntitiesBulk(this._curBulk);
                this._curBulk = [];
            }

            callback();
        }
        catch (error) {
            callback(error);
        }
    }

    async _final(callback) {
        try {
            if (this._curBulk.length > 0) {
                if (!this._apidbWriter.inited) {
                    await this._apidbWriter.initWrite();
                }

                await this._apidbWriter.writeEntitiesBulk(this._curBulk);
            }

            await this._apidbWriter.endWrite();
            callback();
        }
        catch (error) {
            callback(error);
        }
    }
}

module.exports = OsmApidbWriter;
