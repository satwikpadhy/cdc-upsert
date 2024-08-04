This project enables us to upsert incoming data to an postgres table.

The table must have an column named "op" and the file with incoming changes also needs to have the column "op"
Different values for op mean the following:
  1. U : An existing record needs to be updated (on the basis of primary key)
  2. I : A new record needs to be inserted
  3. D : An existing record need to be deleted (on the basis of primary key)

Both the table and the file with must have a column that captures the timestamp at which the change was captured. This column name is feeded into the precombinedkeys variable.

The table must have primary key(s)
