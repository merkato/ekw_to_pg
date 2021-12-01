-- ekw.r02 definition

-- Drop table

-- DROP TABLE ekw.r02;

CREATE TABLE ekw.r02 (
	kw varchar NOT NULL,
	stankw varchar NULL,
	zapisaniekw varchar NULL,
	ujawnieniekw varchar NULL,
	dotychczasowakw varchar NULL,
	znacznikczasu timestamptz default now(),
	CONSTRAINT r02_pk PRIMARY KEY (kw)
);


-- ekw.d1or13 definition

-- Drop table

-- DROP TABLE ekw.d1or13;

CREATE TABLE ekw.d1or13 (
	kw varchar NOT NULL,
	lp varchar NULL,
	wojewodztwo varchar NULL,
	powiat varchar NULL,
	gmina varchar NULL,
	miejscowosc varchar NULL,
	znacznikczasu timestamptz default now(),
	CONSTRAINT d1or13_fk FOREIGN KEY (kw) REFERENCES ekw.r02(kw)
);


-- ekw.d1or14 definition

-- Drop table

-- DROP TABLE ekw.d1or14;

CREATE TABLE ekw.d1or14 (
	kw varchar NOT NULL,
	iddzialki varchar NULL,
	numerdzialki varchar NULL,
	sposobko varchar NULL,
	przylaczenie varchar NULL,
	lpn varchar NULL,
	znacznikczasu timestamptz default now(),
	CONSTRAINT d1or14_fk FOREIGN KEY (kw) REFERENCES ekw.r02(kw)
);


-- ekw.d2r22 definition

-- Drop table

-- DROP TABLE ekw.d2r22;

CREATE TABLE ekw.d2r22 (
	kw varchar NOT NULL,
	zarzad varchar NULL,
	wlasciciel varchar NULL,
	rola varchar NULL,
	CONSTRAINT d2r22_fk FOREIGN KEY (kw) REFERENCES ekw.r02(kw)
);
