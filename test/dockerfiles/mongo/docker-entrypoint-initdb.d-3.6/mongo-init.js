print("Adding travis username to ee2 database for mongo 3.6")
db.auth('travis', 'travis')
db = db.getSiblingDB('ee2')
db.createUser(
        {
            user: "travis",
            pwd: "travis",
            roles: [
                {
                    role: "dbOwner",
                    db: "ee2"
                }
            ]
        }
);