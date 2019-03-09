
function _buildWayNodesHistoryInsert(entity) {
    const id = entity.attributes.id;
    const version = entity.attributes.version;
    const nds = entity.nds;

    let statement = 'INSERT INTO way_nodes (way_id, node_id, sequence_id, version) VALUES ';

    for (let seq = 0; seq < nds.length; seq++) {
        statement += `(${id}, ${nds[seq]}, ${seq + 1}, ${version}),`;
    }

    return statement.slice(0, -1);
}

function _buildWayNodesCurrentInsert(entity) {
    const id = entity.attributes.id;
    const nds = entity.nds;

    let statement = 'INSERT INTO current_way_nodes (way_id, node_id, sequence_id) VALUES ';

    for (let seq = 0; seq < nds.length; seq++) {
        statement += `(${id}, ${nds[seq]}, ${seq + 1}),`;
    }

    return statement.slice(0, -1);
}

function _buildWayNodesCurrentDelete(entity) {
    return `DELETE FROM current_way_nodes 
            WHERE way_id = ${entity.attributes.id}`;
}

module.exports = (entitiesBulk, _, pgStatements) => {
    for (const entity of entitiesBulk) {
        if (entity.type === 'way') {
            if (entity.action && ['modify', 'delete'].includes(entity.action)) {
                pgStatements.regular.push(_buildWayNodesCurrentDelete(entity));
            }

            if (entity.nds && entity.nds.length > 0 && entity.action !== 'delete') {
                pgStatements.regular.push(_buildWayNodesHistoryInsert(entity));
                pgStatements.regular.push(_buildWayNodesCurrentInsert(entity));
            }
        }
    }
}
