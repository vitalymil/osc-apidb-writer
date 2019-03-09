
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

        const pgStatements = {
            regular: [],
            parameterized: [],
        };

        for (const statemantCreator of require('./statement-creators')) {
            await statemantCreator(entitiesBulk, this._pgExecute.bind(this), pgStatements);
        }

        await this._writeStatements(pgStatements);
    }

    async endWrite() {
        await this._pgExecute('commit');
        await this._client.release();
        this._client = null;
        console.log(new Date().getTime());
    }

    async _writeStatements(statements) {
        await this._pgExecute(statements.regular.reduce((acc, cur) => acc + cur + ';\n', ''));

        for (const paramStatement of statements.parameterized) {
            let paramsCount = 0;
            await this._pgExecute(
                paramStatement.statement
                    .replace(/\?/g, () => `$${++paramsCount}`),
                    paramStatement.parameters);
        }
    }

    async _pgExecute(statement, parameters) {
        try {
            return (await this._client.query(statement, parameters)).rows;
        }
        catch(e) {
            console.log(e);
        }
    }
}

module.exports = ApidbBulkWriter;
