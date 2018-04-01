create table if not exists dados_de_compras(
	CPF varchar NOT NULL primary key, 
	PRIVATE smallint NOT NULL,
	INCOMPLETO smallint NOT NULL, 
	DT_ULTIMA_COMPRA date,
	TICKET_MEDIO numeric(10,2), 
	TICKET_ULTIMA_COMPRA numeric(10,2), 
	LOJA_MAIS_FREQUENTADA varchar,
	ULTIMA_LOJA varchar 
)