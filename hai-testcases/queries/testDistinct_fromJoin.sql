SELECT DISTINCT Customer.gender,Cart.cartid
FROM Customer,Cart
WHERE Customer.cid=Cart.cid