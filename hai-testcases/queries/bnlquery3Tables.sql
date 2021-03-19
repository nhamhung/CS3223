SELECT DISTINCT Cart.cartid,Customer.cid,Customer.gender
FROM Customer,Cart,CartDetails
WHERE Customer.cid=Cart.cid,Cart.cartid=CartDetails.cartid,Customer.cid>"150"