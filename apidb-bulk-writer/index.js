
const pg = require('pg');

class ApidbBulkWriter {
    constructor(options) {
        if (options.pgClient) {
            this._client = {
                query: options.queryMethod,
            };
        }
        else {
            this._pool = new pg.Pool(options.pgConnectionProperties);
        }

        this._inited = false;
    }

    get inited() {
        return this._inited;
    }

    async initWrite() {
        if (this._pool) {
            this._client = await this._pool.connect();
        }

        await this._pgExecute('begin');
        this._inited = true;
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
        if (this._pool) {
            await this._pgExecute('commit');
            await this._client.release();
            
            this._client = null;
        }

        this._inited = false;
    }

    async _writeStatements(statements) {
        let currentBulk = '';

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
