SELECT Customer.cid,Cart.cartid,Customer.firstname,Customer.gender,Cart.status
FROM Customer,Cart
WHERE Customer.cid>Cart.cartid,Cart.cartid>"450"
ORDERBY Customer.cid,Cart.cartid
