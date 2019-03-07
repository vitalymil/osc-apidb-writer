
const Writable = require('stream').Writable;
const ApidbBulkWriter = require('./apidb-bulk-writer');

const DEFAULT_BULK_SIZE = 3;

class OsmApidbWriter extends Writable {
    constructor(options = {}) {
        super({ objectMode: true });

        this._curBulk = [];
        this._bulkSize = options.bulkSize || DEFAULT_BULK_SIZE;
        this._apidbWriter = new ApidbBulkWriter();
    }

    async _write(chunk, encoding, callback) {
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

    async _final(callback) {
        if (this._curBulk.length > 0) {
            if (!this._apidbWriter.inited) {
                await this._apidbWriter.initWrite();
            }

            await this._apidbWriter.writeEntitiesBulk(this._curBulk);
            await this._apidbWriter.endWrite();
        }

        callback();
    }
}

module.exports = OsmApidbWriter;
