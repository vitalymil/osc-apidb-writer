
module.exports = async (entitiesBulk, pgExecuteMethod, pgStatements) => {
    const uids = {};
    const paramStatement = {
        statement: `INSERT INTO users (id, email, pass_crypt, creation_time, 
                        display_name, data_public, description, home_lat, 
                        home_lon, home_zoom, nearby, pass_salt) 
                    VALUES `,
        parameters: [],
    }
    
    for (const entity of entitiesBulk) {
        const uid = entity.attributes.uid || '-1';
        const user = entity.attributes.user && entity.attributes.uid ? 
            entity.attributes.user : 
            'anonymous';
        uids[uid] = `osmapidbw_user_${uid} (${user})`;
    }

    const existingUsers = 
        await pgExecuteMethod('SELECT id FROM users WHERE id = ANY ($1)', [Object.keys(uids)]);

    for (const existingUser of existingUsers) {
        delete uids[existingUser.id];
    }

    for (const uid in uids) {
        paramStatement.statement +=
            `(?, ?, '00000000000000000000000000000000', NOW(), 
              ?, true, ?, 0, 0, 3, 50, '00000000'),`

        paramStatement.parameters.push(
            uid, `oscapidbw_user_${uid}@email.com`,
            uids[uid], uids[uid]
        );
    }

    if (paramStatement.parameters.length > 0) {
        paramStatement.statement = paramStatement.statement.slice(0, -1);
        pgStatements.parameterized.push(paramStatement);
    }
}
