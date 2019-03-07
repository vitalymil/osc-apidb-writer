
function _buildTagsHistoryInsert(entity) {
    const id = entity.attributes.id;
    const version = entity.attributes.version;
    const type = entity.type;
    const tags = entity.tags;

    let statement = `INSERT INTO ${type}_tags (${type}_id, k, v, version) VALUES `;

    for (const tag of tags) {
        statement += `(${id}, '${tag.k}', '${tag.v}', ${version}),`;
    }

    return statement.slice(0, -1);
}

function _buildTagsCurrentInsert(entity) {
    const id = entity.attributes.id;
    const type = entity.type;
    const tags = entity.tags;

    let statement = `INSERT INTO current_${type}_tags (${type}_id, k, v) VALUES `;

    for (const tag of tags) {
        statement += `(${id}, '${tag.k}', '${tag.v}'),`;
    }

    return statement.slice(0, -1);
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

        if (entity.tags && entity.tags.length > 0 && entity.action !== 'delete') {
            pgStatements.push(_buildTagsHistoryInsert(entity));
            pgStatements.push(_buildTagsCurrentInsert(entity));
        }
    }
}
