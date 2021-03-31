SAVE_PATH="$PATH"
export PATH=$TEST_ACTIVATE_PATH:$PATH
rm test.res
./test_execute.sh > test.res
echo "Comparing the result of the execution with the expected result"
diff test_activate.res test.res
PATH="$SAVE_PATH"
