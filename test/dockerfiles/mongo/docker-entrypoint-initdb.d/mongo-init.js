print("Adding travis username to ee2 database")

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
