
const pg = reuqire('pg');

class ApidbBulkWriter {
    constructor(pgConnectionProperties) {
        this._inited = false;
        this._pool = new pg.Pool(pgConnectionProperties);
    }

    get inited() {
        return this._inited;
    }

    async initWrite() {
        await this._pgExecute('begin');
        this._inited = true;
    }

    async writeEntitiesBulk(entitiesBulk) {
        if (!this._inited) {
            throw new Error('ApidbBulkWriter cannot write, need to call initWrite method first');
        }

        const pgStatements = [];

        for (const action of require('./statement-creators')) {
            await action(entitiesBulk, this._pgExecute, pgStatements);
        }

        await this._writeStatements(pgStatements);
    }

    async endWrite() {
        await this._pgExecute('commit');
        this._inited = false;
    }

    async _writeStatements(statements) {
        this._pgExecute(statements.reduce((acc, cur) => acc + cur + ';\n', ''));
    }

    async _pgExecute(statement, parameters) {
        const client = await this._pool.connect();

        try {
            return (await client.query(statement, parameters)).rows;
        }
        finally {
            client.release();
        }
    }
}
