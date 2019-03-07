
const pointUtils = require('../../point-utils');

function _buildEntityHistoryInsert(entity) {
    const attrs = entity.attributes;

    return `INSERT INTO ${entity.type}s(${entity.type}_id, 
                timestamp, version, visible, 
                changeset_id${entity.type === 'node' ? ', latitude, longitude, tile' : ''})
            VALUES
                (${attrs.id}, to_timestamp('${attrs.timestamp}', 'YYYY-MM-DD"T"hh24:mi:ss"Z"'),
                ${attrs.version}, ${attrs.visible}, ${attrs.changeset}
                ${entity.type === 'node' ? `, 
                    ${pointUtils.convertToFixed(attrs.lat)}, 
                    ${pointUtils.convertToFixed(attrs.lon)}, 
                    ${pointUtils.calculateTile(attrs.lat, attrs.lon)}` : ''})`;
}

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

module.exports = (entitiesBulk, _, pgStatements) => {
    for (const entity of entitiesBulk) {
        pgStatements.push(_buildEntityHistoryInsert(entity));

        if (entity.tags && entity.tags.length > 0) {
            pgStatements.push(_buildTagsHistoryInsert(entity));
        }
    }
}
