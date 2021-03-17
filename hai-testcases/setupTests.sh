cd tables || { echo "Cannot cd into tables/"; exit 1; }
java RandomDB Customer 500
java RandomDB CartDetails 1000
java RandomDB Cart 500
java RandomDB EmptyCustomer 0
java RandomDB EmptyCart 0
java ConvertTxtToTbl Customer
java ConvertTxtToTbl CartDetails
java ConvertTxtToTbl Cart
java ConvertTxtToTbl EmptyCustomer
java ConvertTxtToTbl EmptyCart
cd .. || { echo "Error cd out of tables/"; exit 1; }