cd tables || { echo "Error cd into tables"; exit 1;}

echo "1000\n100\n1\n" | java QueryMain ../queries/joinEmployeesAndCertified.sql ../output/joinEmployeesAndCertified.out # generic 115 values

cd .. || { echo "Error cd into tables"; exit 1;}