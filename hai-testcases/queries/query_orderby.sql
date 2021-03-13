SELECT Customer.gender,Cart.cartid,Customer.cid
FROM Customer,Cart
WHERE Customer.cid=Cart.cid,Customer.cid>"150"
ORDERBY Customer.gender,Cart.cartid