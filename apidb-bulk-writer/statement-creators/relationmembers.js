
function _typeToPgEnum(type) {
    switch(type) {
        case 'node': return `'Node'`;
        case 'way': return `'Way'`;
        case 'relation': return `'Relation'`;
    }
}

function _buildRelationMembersHistoryInsert(entity) {
    const id = entity.attributes.id;
    const version = entity.attributes.version;
    const members = entity.members;

    let statement = `INSERT INTO relation_members (relation_id, member_type, 
        member_id, sequence_id, member_role, version) VALUES `;

    for (let seq = 0; seq < members.length; seq++) {
        statement += `(${id}, ${_typeToPgEnum(members[seq].type)}, 
        ${members[seq].id}, ${seq + 1}, '${members[seq].role}', ${version}),`;
    }

    return statement.slice(0, -1);
}

function _buildRelationMembersCurrentInsert(entity) {
    const id = entity.attributes.id;
    const members = entity.members;

    let statement = `INSERT INTO current_relation_members (relation_id, member_type, 
        member_id, sequence_id, member_role) VALUES `;

    for (let seq = 0; seq < members.length; seq++) {
        statement += `(${id}, ${_typeToPgEnum(members[seq].type)}, 
        ${members[seq].id}, ${seq + 1}, '${members[seq].role}'),`;
    }

    return statement.slice(0, -1);
}

function _buildRelationMembersCurrentDelete(entity) {
    return `DELETE FROM current_relation_members 
            WHERE relation_id = ${entity.attributes.id}`;
}

module.exports = (entitiesBulk, _, pgStatements) => {
    for (const entity of entitiesBulk) {
        if (entity.type === 'relation') {
            if (entity.action && ['modify', 'delete'].includes(entity.action)) {
                pgStatements.push(_buildRelationMembersCurrentDelete(entity));
            }

            if (entity.members && entity.members.length > 0 && entity.action !== 'delete') {
                pgStatements.push(_buildRelationMembersHistoryInsert(entity));
                pgStatements.push(_buildRelationMembersCurrentInsert(entity));
            }
        }
    }
}
