
module.exports = async (entitiesBulk, pgExecuteMethod, pgStatements) => {
    const uids = {};
    
    for (const entity of entitiesBulk) {
        uids[entity.attributes.uid] = entity.attributes.user;
    }

    const existingUsers = 
        await pgExecuteMethod('SELECT id FROM users WHERE id = ANY ($1)', [Object.keys(uids)]);

    for (const existingUser of existingUsers) {
        delete uids[existingUser.id];
    }

    for (const uid in uids) {
        pgStatements.push(
            `INSERT INTO users (id, email, pass_crypt, creation_time, 
                display_name, data_public, description, home_lat, 
                home_lon, home_zoom, nearby, pass_salt) 
            VALUES (${uid}, 'oscapidbw_user_${uid}@email.com', 
                '00000000000000000000000000000000', NOW(), '${uids[uid]}', true,
                '${uids[uid]}', 0, 0, 3, 50, '00000000')`
        )
    }
}
