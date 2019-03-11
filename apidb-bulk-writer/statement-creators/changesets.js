
const pointUtils = require('../../point-utils');

const MIN_LAT = pointUtils.convertToFixed(-90);
const MAX_LAT = pointUtils.convertToFixed(90);
const MIN_LON = pointUtils.convertToFixed(-180);
const MAX_LON = pointUtils.convertToFixed(180);

module.exports = async (entitiesBulk, pgExecuteMethod, pgStatements) => {
    const changesetUids = {};
    const changesetArray = [];

    for (const entity of entitiesBulk) {
        changesetArray.push(entity.attributes.changeset);
        changesetUids[entity.attributes.changeset] = { 
            uid: entity.attributes.uid || '-1', 
            time: entity.attributes.timestamp 
        };
    }

    const changesets = new Set(changesetArray);
    const existingChangesets = 
        await pgExecuteMethod('SELECT id FROM changesets WHERE id = ANY ($1)', [changesetArray]);

    for (const existingChangeset of existingChangesets) {
        changesets.delete(existingChangeset.id);
    }

    for (const changeset of changesets) {
        pgStatements.push(
            `INSERT INTO changesets
                (id, user_id, created_at, min_lat, max_lat, min_lon, 
                max_lon, closed_at, num_changes) 
            VALUES
                (${changeset}, ${changesetUids[changeset].uid}, 
                to_timestamp('${changesetUids[changeset].time}', 'YYYY-MM-DD"T"hh24:mi:ss"Z"'), 
                ${MIN_LAT}, ${MAX_LAT}, ${MIN_LON}, ${MAX_LON}, 
                to_timestamp('${changesetUids[changeset].time}', 'YYYY-MM-DD"T"hh24:mi:ss"Z"'), 0)`
        );
    }
}
