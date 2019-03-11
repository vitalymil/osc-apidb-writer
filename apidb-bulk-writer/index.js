
const pg = require('pg');

class ApidbBulkWriter {
    constructor(pgConnectionProperties) {
        this._pool = new pg.Pool(pgConnectionProperties);
        this._client = null;
    }

    get inited() {
        return !!this._client;
    }

    async initWrite() {
        this._client = await this._pool.connect();
        await this._pgExecute('begin');
    }

    async writeEntitiesBulk(entitiesBulk) {
        if (!this.inited) {
            throw new Error('ApidbBulkWriter cannot write, need to call initWrite method first');
        }

        const pgStatements = [];

        for (const statemantCreator of require('./statement-creators')) {
            await statemantCreator(entitiesBulk, this._pgExecute.bind(this), pgStatements);
        }

        await this._writeStatements(pgStatements);
    }

    async endWrite() {
        await this._pgExecute('commit');
        await this._client.release();
        this._client = null;
    }

    async _writeStatements(statements) {
        const currentBulk = '';

        for (const statement of statements) {
            if (statement.parameters) {
                if (currentBulk.length > 0) {
                    await this._pgExecute(currentBulk);
                    currentBulk = '';
                }

                let paramsCount = 0;
                await this._pgExecute(
                    statement.statement
                        .replace(/\?/g, () => `$${++paramsCount}`),
                        statement.parameters);
            }
            else {
                currentBulk += statement + ';\n';
            }
        }

        if (currentBulk.length > 0) {
            await this._pgExecute(currentBulk);
        }
    }

    async _pgExecute(statement, parameters) {
        return (await this._client.query(statement, parameters)).rows;
    }
}

module.exports = ApidbBulkWriter;
