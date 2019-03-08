
const pointUtils = require('../../point-utils');

function _buildEntityInsert(entity, isCurrent) {
    const attrs = entity.attributes;
    const tableName = isCurrent ?
                        `current_${entity.type}s`:
                        `${entity.type}s`;

    return `INSERT INTO ${tableName} (
                ${isCurrent ? '' : entity.type + '_'}id, 
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

function _buildEntityCurrentUpdate(entity) {
    const attrs = entity.attributes;

    return `UPDATE current_${entity.type}s 
            SET 
            version = ${attrs.version},
            timestamp = to_timestamp('${attrs.timestamp}', 'YYYY-MM-DD"T"hh24:mi:ss"Z"'), 
            visible = ${entity.action !== 'delete'}, changeset_id = ${attrs.changeset}
            ${entity.type === 'node' ? `, latitude = ${entity.computedAttributes.lat}, 
                                          longitude = ${entity.computedAttributes.lon}, 
                                          tile = ${entity.computedAttributes.tile}` : ''}
            WHERE id = ${attrs.id}`;
}

module.exports = (entitiesBulk, _, pgStatements) => {
    for (const entity of entitiesBulk) {
        entity.computedAttributes = {
            lat: pointUtils.convertToFixed(entity.attributes.lat),
            lon: pointUtils.convertToFixed(entity.attributes.lon),
            tile: pointUtils.calculateTile(entity.attributes.lat, entity.attributes.lon)
        };

        pgStatements.push(_buildEntityInsert(entity, false));

        if (entity.action && ['modify', 'delete'].includes(entity.action)) {
            pgStatements.push(_buildEntityCurrentUpdate(entity));
        }
        else {
            pgStatements.push(_buildEntityInsert(entity, true));
        }
    }
}
