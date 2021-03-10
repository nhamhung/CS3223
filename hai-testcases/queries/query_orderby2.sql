SELECT Customer.gender,Customer.cid,Cart.cartid
FROM Customer,Cart
WHERE Customer.cid=Cart.cid
ORDERBY Customer.gender,Customer.cid,Cart.cartid