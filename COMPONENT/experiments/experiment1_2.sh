cd tables || { echo "Error cd into tables"; exit 1;}
echo "=================================="
echo "========= STARTING BNL ==========="
echo "=================================="

echo "JOINING EMPLOYEES AND CERTIFIED"
echo "buffer size: 200 - buffer pages: 3"
echo "200\n3\n1\n" | java QueryMain ../queries/joinEmployeesAndCertified.sql ../output/joinEmployeesAndCertifiedFlipped_200-3.out
echo "----------------------------------"
echo "buffer size: 5000 - buffer pages: 3"
echo "5000\n3\n1\n" | java QueryMain ../queries/joinEmployeesAndCertified.sql ../output/joinEmployeesAndCertifiedFlipped_5000-3.out
echo "----------------------------------"
echo "buffer size: 200 - buffer pages: 200"
echo "200\n200\n1\n" | java QueryMain ../queries/joinEmployeesAndCertified.sql ../output/joinEmployeesAndCertifiedFlipped_200-200.out
echo "----------------------------------"
echo "buffer size: 5000 - buffer pages: 200"
echo "5000\n200\n1\n" | java QueryMain ../queries/joinEmployeesAndCertified.sql ../output/joinEmployeesAndCertifiedFlipped_5000-200.out

cd .. || { echo "Error cd into tables"; exit 1;}