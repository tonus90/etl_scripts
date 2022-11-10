CREATE TABLE if not exists core.h_orders (
	orderid int4 NOT NULL,
    load_date timestamp default current_timestamp,
    rec_source varchar(20),
	CONSTRAINT orders_pk PRIMARY KEY (orderid)
);

CREATE TABLE if not exists core.s_orders (
	orderid int4 NOT NULL,
	orderdate date NULL,
	orderstatus varchar(1) NULL,
	orderpriority varchar(15) NULL,
	clerk varchar(15) NULL,
    load_date timestamp default current_timestamp,
    rec_source varchar(20),
	CONSTRAINT orders_fk FOREIGN KEY (orderid) REFERENCES core.h_orders(orderid)
);

CREATE TABLE if not exists core.h_products (
	productid int4 NOT NULL,
    load_date timestamp default current_timestamp,
    rec_source varchar(20),
	CONSTRAINT products_pk PRIMARY KEY (productid)
);

CREATE TABLE if not exists core.s_products (
	productid int4 NOT NULL,
	productname varchar(55) NULL,
	producttype varchar(25) NULL,
	productsize int4 NULL,
	retailprice numeric(15,2) NULL,
    load_date timestamp default current_timestamp,
    rec_source varchar(20),
	CONSTRAINT products_fk FOREIGN KEY (productid) REFERENCES core.h_products(productid)
);


CREATE TABLE if not exists core.h_suppliers (
	supplierid int4 NOT NULL,
    load_date timestamp default current_timestamp,
    rec_source varchar(20),
	CONSTRAINT supplier_pk PRIMARY KEY (supplierid)
);

CREATE TABLE if not exists core.s_suppliers (
	supplierid int4 NOT NULL,
	suppliername varchar(25) NULL,
	address varchar(40) NULL,
	phone varchar(15) NULL,
	balance numeric(15,2) NULL,
	descr varchar(101) NULL,
    load_date timestamp default current_timestamp,
    rec_source varchar(20),
	CONSTRAINT suppliers_fk FOREIGN KEY (supplierid) REFERENCES core.h_suppliers(supplierid)
);

CREATE TABLE if not exists core.l_orders_products (
    orderid int4 NOT NULL,
	productid int4 NOT NULL,
    load_date timestamp default current_timestamp,
    rec_source varchar(20),
    CONSTRAINT orderdetails_pk PRIMARY KEY (orderid, productid),
	CONSTRAINT orderdetails_fk FOREIGN KEY (orderid) REFERENCES core.h_orders(orderid),
	CONSTRAINT orderdetails_fk_1 FOREIGN KEY (productid) REFERENCES core.h_products(productid)
);

CREATE TABLE if not exists core.s_orders_products (
    orderid int4 NOT NULL,
	productid int4 NOT NULL,
	unitprice numeric(15,2) NULL,
	quantity numeric(15,2) NULL,
	discount numeric(15,2) NULL, 
    load_date timestamp default current_timestamp,
    rec_source varchar(20),
    CONSTRAINT orderdetails_l_fk FOREIGN KEY (orderid, productid) REFERENCES core.l_orders_products (orderid, productid)
);

CREATE TABLE if not exists core.l_product_suppl (
	productid int4 NOT NULL,
	supplierid int4 NOT NULL,
	load_date timestamp default current_timestamp,
    rec_source varchar(20),
	CONSTRAINT productsuppl_pk PRIMARY KEY (productid, supplierid),
	CONSTRAINT productsuppl_fk FOREIGN KEY (productid) REFERENCES core.h_products(productid),
	CONSTRAINT productsuppl_fk_1 FOREIGN KEY (supplierid) REFERENCES core.h_suppliers(supplierid)
);


CREATE TABLE if not exists core.s_product_suppl (
	productid int4 NOT NULL,
	supplierid int4 NOT NULL,
	qty int4 NULL,
	supplycost numeric(15,2) NULL,
	descr varchar(199) NULL,
	load_date timestamp default current_timestamp,
    rec_source varchar(20),
	CONSTRAINT productsuppl_l_fk FOREIGN KEY (productid, supplierid) REFERENCES core.l_product_suppl (productid, supplierid)
);


