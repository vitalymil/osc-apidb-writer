
const pointUtils = require('../../point-utils');

function _buildEntityHistoryInsert(entity) {
    const attrs = entity.attributes;

    return `INSERT INTO ${entity.type}s (
                ${entity.type}_id, 
                timestamp, version, visible, 
                changeset_id${entity.type === 'node' ? ', latitude, longitude, tile' : ''})
            VALUES
                (${attrs.id}, to_timestamp('${attrs.timestamp}', 'YYYY-MM-DD"T"hh24:mi:ss"Z"'),
                ${attrs.version}, ${entity.action !== 'delete'}, ${attrs.changeset}
                ${entity.type === 'node' ? `, 
                    ${entity.computedAttributes.lat}, 
                    ${entity.computedAttributes.lon}, 
                    ${entity.computedAttributes.tile}` : ''})`;
}

function _buildEntityCurrentUpsert(entity) {
    const attrs = entity.attributes;

    return `INSERT INTO current_${entity.type}s (
                id, timestamp, version, visible, 
                changeset_id${entity.type === 'node' ? ', latitude, longitude, tile' : ''})
            VALUES
                (${attrs.id}, to_timestamp('${attrs.timestamp}', 'YYYY-MM-DD"T"hh24:mi:ss"Z"'),
                ${attrs.version}, ${entity.action !== 'delete'}, ${attrs.changeset}
                ${entity.type === 'node' ? `, 
                    ${entity.computedAttributes.lat}, 
                    ${entity.computedAttributes.lon}, 
                    ${entity.computedAttributes.tile}` : ''})
            ON CONFLICT (id) DO UPDATE
            SET
            version = ${attrs.version},
            timestamp = to_timestamp('${attrs.timestamp}', 'YYYY-MM-DD"T"hh24:mi:ss"Z"'), 
            visible = ${entity.action !== 'delete'}, changeset_id = ${attrs.changeset}
            ${entity.type === 'node' ? `, latitude = ${entity.computedAttributes.lat}, 
                                          longitude = ${entity.computedAttributes.lon}, 
                                          tile = ${entity.computedAttributes.tile}` : ''}`;
}

module.exports = (entitiesBulk, _, pgStatements) => {
    for (const entity of entitiesBulk) {
        if (entity.attributes.lat && entity.attributes.lon) {
            entity.computedAttributes = {
                lat: pointUtils.convertToFixed(entity.attributes.lat),
                lon: pointUtils.convertToFixed(entity.attributes.lon),
                tile: pointUtils.calculateTile(entity.attributes.lat, entity.attributes.lon)
            };
        }

        pgStatements.regular.push(_buildEntityHistoryInsert(entity));
        pgStatements.regular.push(_buildEntityCurrentUpsert(entity));
    }
}
