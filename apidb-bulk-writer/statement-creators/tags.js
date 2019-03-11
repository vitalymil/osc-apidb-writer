
const MAX_PARAMETERS = 65500;

function _buildTagsInsertBulk(entitiesBulk, pgStatements, isHistory) {
    let paramStatements = {};

    for (const entity of entitiesBulk) {
        if (entity.tags && entity.tags.length > 0 && entity.action !== 'delete') {
            let paramStatement = paramStatements[entity.type];

            if (paramStatement && 
                paramStatement.parameters.length + 
                    (entity.tags.length * (isHistory ? 4 : 3)) > MAX_PARAMETERS) 
            {
                paramStatement.statement = paramStatement.statement.slice(0, -1);
                pgStatements.push(paramStatement);
                paramStatement = null;
            }

            if (!paramStatement) {
                paramStatement = { 
                    statement: `INSERT INTO ${!isHistory ? 'current_' : ''}${entity.type}_tags 
                                (${entity.type}_id, k, v${isHistory ? ', version' : ''}) VALUES `,
                    parameters: [],
                };

                paramStatements[entity.type] = paramStatement;
            }

            _buildTagsInsert(entity, paramStatement, isHistory);
        }
    }

    for (const type in paramStatements) {
        const paramStatement = paramStatements[type];

        if (paramStatement && paramStatement.parameters.length > 0) {
            paramStatement.statement = paramStatement.statement.slice(0, -1);
            pgStatements.push(paramStatement);
        }
    }
}

function _buildTagsInsert(entity, paramStatement, isHistory) {
    const id = entity.attributes.id;
    const version = entity.attributes.version;
    const tags = entity.tags;

    for (const tag of tags) {
        paramStatement.statement += 
            isHistory ? `(?, ?, ?, ?),` : `(?, ?, ?),`;

        paramStatement.parameters.push(id, tag.k, tag.v);

        if (isHistory) {
            paramStatement.parameters.push(version);
        }
    }
}

function _buildTagsCurrentDelete(entity) {
    return `DELETE FROM current_${entity.type}_tags 
            WHERE ${entity.type}_id = ${entity.attributes.id}`;
}

module.exports = (entitiesBulk, _, pgStatements) => {
    for (const entity of entitiesBulk) {
        if (entity.action && ['modify', 'delete'].includes(entity.action)) {
            pgStatements.push(_buildTagsCurrentDelete(entity));
        }
    }

    _buildTagsInsertBulk(entitiesBulk, pgStatements, true);
    _buildTagsInsertBulk(entitiesBulk, pgStatements, false);
}
