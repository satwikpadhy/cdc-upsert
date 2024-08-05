This project enables us to upsert incoming data to a postgres table.

The table must have a column named "operation" and the file with incoming changes also needs to have the column "operation"
Different values for operation mean the following:
  1. U : An existing record needs to be updated (on the basis of primary key)
  2. I : A new record needs to be inserted
  3. D : An existing record need to be deleted (on the basis of primary key)

Both the table and the file with must have a column that captures the timestamp at which the change was captured. This column name is fed into the timestamp_keys variable.

The table must have primary key(s)

Input arguments to the jar:
    1. csvFile
    2. ip
    3. port
    4. database
    5. tablename
    6. user
    7. password
    8. primarykeys
    9. timestamp_keys
