print("Adding travis username to ee2 database for mongo 7.0")
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