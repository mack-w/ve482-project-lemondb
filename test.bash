mkdir -p build
cd build
arr2=(1 2 3 4)
pip install numpy
echo "">../data
for x in "${arr2[@]}"
do
arr=( "few_insert_delete" "few_read_dup" "few_read_update" "few_read" "many_insert_delete" "many_read_dup" "many_read_update" "many_read" "single_insert_delete" "single_read_dup" "single_read_update" "single_read" "test")
echo "newTest" >>../data
for i in "${arr[@]}"
do
echo -e "\e[1m${i}\e[0m"
echo -e "${i}">>../data
(time ./lemondb --listen ../sample/${i}.query >../sample/${i}.query.out 2>/dev/null) 2>> ../data
# perf stat ./lemondb <../sample/${i}.query >../sample/${i}.query.out
(time ../bin/lemondb --listen ../sample/${i}.query >./tmp.out 2>/dev/null) 2>> ../data
echo "" 
echo "---------- diff output ----------"
diff ../sample/${i}.query.out ../sample_stdout/${i}.out
echo ""
echo ""
done
cat ../data
cd ..
python3 performance.py
cd build
done