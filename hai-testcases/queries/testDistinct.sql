SELECT DISTINCT Customer.firstname
FROM Customer,Cart
WHERE Customer.cid=Cart.cid