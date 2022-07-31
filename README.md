**CSE-5331-001**\
**Project-1**\
**Implementation of Two-Phase Locking(2PL) Protocol**\
By : Krishna Kabra(1001867795), 
Shubham Simant(1001860599)

**Instructions:**\
1. Keep input.txt and output.txt files in same folder of TwoPhaseLocking.
2. Kindly empty or delete the output file before re-executing the code.

**Requirements:**\
1. import copy, re, sys

**Execution:**
1. Command run program : \
'python3 Main.py _input file.txt output file.txt_'
2. Check output file.txt generated in same folder.

**File Structure:**
1. Constants.py has all global constants related to state of resource, transaction and type of operation. 
2. Main.py has following the functions:
begin(), start_execution(), execute(), read_lock(), write_lock(), commit(), unlock_resource(),
abort(), wait_die().
3. Lock.py: Class to represent single locked resource.
4. Transaction.py: Class to represent single transaction.

