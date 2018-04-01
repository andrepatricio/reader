package main

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"

	_ "github.com/lib/pq"
)

const (
	host     = "db"
	port     = 5432
	user     = "docker"
	password = "docker"
	dbname   = "docker"
)

type DadosDeCompra struct {
	CPF                string
	Private            int
	Incompleto         int
	DataUltimaCompra   string
	TicketMedio        float32
	TicketUltimaCompra float32
	LojaMaisFrequente  string
	LojaUltimaCompra   string
}

func parse(n int, linha string) DadosDeCompra {
	valores := strings.Fields(string(linha))
	var dados DadosDeCompra
	dados.CPF = valores[0]
	private, err := strconv.Atoi(valores[1])
	if err == nil {
		dados.Private = private
	}
	incompleto, err := strconv.Atoi(valores[2])
	if err == nil {
		dados.Incompleto = incompleto
	}
	dados.DataUltimaCompra = valores[3]
	ticketMedio, err := strconv.ParseFloat(valores[4], 32)
	if err == nil {
		dados.TicketMedio = float32(ticketMedio)
	}
	ultimoTicket, err := strconv.ParseFloat(valores[5], 32)
	if err == nil {
		dados.TicketMedio = float32(ultimoTicket)
	}
	dados.LojaMaisFrequente = valores[6]
	dados.LojaUltimaCompra = valores[7]

	return dados
}

func insert(dados DadosDeCompra) {
	configuracoesBanco := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
	db, err := sql.Open("postgres", configuracoesBanco)
	if err != nil {
		log.Fatal(err)
	}
	sql := "insert into dados_de_compras "
	sql += "(CPF, PRIVATE, INCOMPLETO, DT_ULTIMA_COMPRA, "
	sql += "TICKET_MEDIO, TICKET_ULTIMA_COMPRA, "
	sql += "LOJA_MAIS_FREQUENTADA, ULTIMA_LOJA) values ($1, $2, $3, $4, $5, $6, $7, $8)"
	_, err = db.Exec(sql, dados.CPF, dados.Private, dados.Incompleto,
		dados.DataUltimaCompra, dados.TicketMedio, dados.TicketUltimaCompra,
		dados.LojaMaisFrequente, dados.LojaUltimaCompra)
	if err != nil {
		fmt.Println("Erro na consulta")
		log.Fatal(err)
	}
	db.Close()
}

func find() {
	configuracoesBanco := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
	db, err := sql.Open("postgres", configuracoesBanco)
	if err != nil {
		log.Fatal(err)
	}
	rows, err := db.Query("Select cpf from DADOS_DE_COMPRA")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var cpf string
		rows.Scan(&cpf)
		fmt.Printf("%s \n", cpf)
	}
}

func main() {
	data, err := ioutil.ReadFile("./arquivos/teste2.txt")
	if err != nil {
		fmt.Fprintf(os.Stderr, "dup: %v\n", err)
	}
	for n, line := range strings.Split(string(data), "\n") {
		if n == 0 {
			continue
		}
		if n == 4 {
			break
		}
		fmt.Println("Linha numero: ", n)
		dados := parse(n, line)
		insert(dados)
	}
	find()
}
