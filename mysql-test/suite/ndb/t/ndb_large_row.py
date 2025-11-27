# ndb_large_row.py
num_insert_rows = 5   # Number of rows in the INSERT loop
num_update_rows = 5   # Number of iterations for UPDATE loop
num_delete_rows = 5   # Number of iterations for DELETE loop

num_varbinary_memory = 1990
num_varbinary_disk = 1990
num_binary16 = 115  # 4096 total columns = 1 PK + 1990 + 1900 + 205

total_columns = num_varbinary_memory + num_varbinary_disk + num_binary16

test_filename = "ndb_large_row.test"

with open(test_filename, "w") as f:
    # Header comment
    f.write("-- source include/have_ndb.inc\n")
    f.write("# Generated MTR test file from ndb_large_row.py for a 4096-column NDB table\n\n")
    f.write("CREATE LOGFILE GROUP lg1\n")
    f.write("ADD UNDOFILE 'undofile.dat'\n")
    f.write("INITIAL_SIZE 16M\n")
    f.write("UNDO_BUFFER_SIZE = 16M\n")
    f.write("ENGINE=NDB;\n")

    f.write("CREATE TABLESPACE ts1\n")
    f.write("ADD DATAFILE 'datafile.dat'\n")
    f.write("USE LOGFILE GROUP lg1\n")
    f.write("INITIAL_SIZE 16M\n")
    f.write("ENGINE NDB;\n")

    # CREATE TABLE
    f.write("CREATE TABLE big_table (\n")
    f.write("  id BIGINT NOT NULL\n")
    
    # VARBINARY(11) memory
    for i in range(1, num_varbinary_memory + 1):
        if i % 8 == 1:
            f.write(f"  ")
        f.write(f",c{i} VARBINARY(11) ")
        if i % 8 == 0:
            f.write(f"\n")
    #f.write(f"\n") 
    # VARBINARY(11) STORAGE DISK
    for i in range(num_varbinary_memory + 1, num_varbinary_memory + num_varbinary_disk + 1):
        if i % 8 == 1:
            f.write(f"  ")
        f.write(f",c{i} VARBINARY(11) STORAGE DISK")
        if i % 8 == 0:
            f.write(f"\n")
    
    # BINARY(12)
    for i in range(num_varbinary_memory + num_varbinary_disk + 1, total_columns + 1):
        if i % 8 == 1:
            f.write(f"  ")
        f.write(f",c{i} BINARY(12)")
        if i % 8 == 0:
            f.write(f"\n")
    
    f.write("  , PRIMARY KEY(id)\n")
    f.write(") CHARACTER SET latin1 ENGINE=NDBCLUSTER TABLESPACE ts1;\n\n")
    
    # INSERT loop
    f.write("# INSERT loop\n")
    for row_id in range(1, num_insert_rows + 1):
        f.write(f"INSERT INTO big_table VALUES ({row_id}")
        # Insert values: VARBINARY(11) -> 'valX', BINARY(16) -> X*'A'
        for i in range(1, total_columns + 1):
            if i <= num_varbinary_memory + num_varbinary_disk:
                f.write(f", 'v{i}_{row_id}'")
            else:
                f.write(f", '{chr(65 + (i%26)) * 12}'")  # BINARY(12) pattern
        f.write(");\n")
    
    f.write("\n# UPDATE loop: set all columns\n")
    for row_id in range(1, num_update_rows + 1):
        f.write(f"UPDATE big_table SET ")
        updates = []
        for i in range(1, total_columns + 1):
            if i <= num_varbinary_memory + num_varbinary_disk:
                updates.append(f"c{i}='u{i}_{row_id}'")
            else:
                updates.append(f"c{i}='{chr(90 - (i%26)) * 12}'")
        f.write(", ".join(updates))
        f.write(f" WHERE id={row_id};\n")
    
    f.write("\n# DELETE loop\n")
    for row_id in range(1, num_delete_rows + 1):
        f.write(f"DELETE FROM big_table WHERE id={row_id};\n")

    f.write("DROP TABLE big_table;\n")
    f.write("ALTER TABLESPACE ts1 DROP DATAFILE 'datafile.dat';\n")
    f.write("DROP TABLESPACE ts1;\n")
    f.write("DROP LOGFILE GROUP lg1 engine ndb;\n")
print(f"MTR test file '{test_filename}' generated successfully.")
