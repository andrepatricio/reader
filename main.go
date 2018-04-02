package main

import (
	"database/sql"
	"fmt"
	"io/ioutil"
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
	CPF                                                   string
	Private, Incompleto                                   int
	TicketMedio, TicketUltimaCompra                       sql.NullFloat64
	LojaMaisFrequente, DataUltimaCompra, LojaUltimaCompra sql.NullString
}

func parse(n int, linha string) (DadosDeCompra, error) {
	valores := strings.Fields(string(linha))
	var dados DadosDeCompra
	if !validarCPF(valores[0]) {
		return dados, fmt.Errorf("CPF %s, na linha %d é invalido", valores[0], n)
	}
	dados.CPF = valores[0]
	private, err := strconv.Atoi(valores[1])
	if err == nil {
		dados.Private = private
	}
	incompleto, err := strconv.Atoi(valores[2])
	if err == nil {
		dados.Incompleto = incompleto
	}
	if valores[3] != "NULL" {
		dados.DataUltimaCompra = sql.NullString{valores[3], true}
	} else {
		dados.DataUltimaCompra = sql.NullString{valores[3], false}
	}
	if valores[4] != "NULL" {
		ticketMedio, err := strconv.ParseFloat(strings.Replace(valores[4], ",", ".", -1), 64)
		checkErr(err)
		dados.TicketMedio = sql.NullFloat64{ticketMedio, true}
	} else {
		dados.TicketMedio = sql.NullFloat64{0, false}
	}
	if valores[5] != "NULL" {
		ultimoTicket, err := strconv.ParseFloat(strings.Replace(valores[5], ",", ".", -1), 64)
		checkErr(err)
		dados.TicketUltimaCompra = sql.NullFloat64{ultimoTicket, true}
	} else {
		dados.TicketUltimaCompra = sql.NullFloat64{0, false}
	}
	if valores[6] != "NULL" {
		if !validarCNPJ(valores[6]) {
			fmt.Errorf("CNPJ da loja mais frequente, %s, na linha %d esta invalido", valores[6], n)
		}
		dados.LojaMaisFrequente = sql.NullString{valores[6], true}
	} else {
		dados.LojaMaisFrequente = sql.NullString{valores[6], false}
	}
	if valores[7] != "NULL" {
		if !validarCNPJ(valores[7]) {
			fmt.Errorf("CNPJ da loja da ultima compra, %s, na linha %d esta invalido", valores[7], n)
		}
		dados.LojaUltimaCompra = sql.NullString{valores[7], true}
	} else {
		dados.LojaUltimaCompra = sql.NullString{valores[6], false}
	}
	return dados, nil
}
func insert(dados DadosDeCompra) {
	configuracoesBanco := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
	db, err := sql.Open("postgres", configuracoesBanco)
	checkErr(err)
	sql := "insert into dados_de_compras "
	sql += "(CPF, PRIVATE, INCOMPLETO, DT_ULTIMA_COMPRA, "
	sql += "TICKET_MEDIO, TICKET_ULTIMA_COMPRA, "
	sql += "LOJA_MAIS_FREQUENTADA, ULTIMA_LOJA) values ($1, $2, $3, $4, $5, $6, $7, $8)"
	_, err = db.Exec(sql, dados.CPF, dados.Private, dados.Incompleto,
		dados.DataUltimaCompra, dados.TicketMedio, dados.TicketUltimaCompra,
		dados.LojaMaisFrequente, dados.LojaUltimaCompra)
	checkErr(err)
	db.Close()
}

func main() {
	data, err := ioutil.ReadFile(os.Args[1])
	checkErr(err)
	for n, line := range strings.Split(string(data), "\n") {
		if n == 0 {
			continue
		}
		dados, err := parse(n, line)
		if err == nil {
			insert(dados)
		} else {
			fmt.Println(err)
		}
	}
}
func checkErr(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "dup: %v\n", err)
		panic(err)
	}
}
func validarCNPJ(cnpj string) bool {
	cnpj = strings.Replace(cnpj, ".", "", -1)
	cnpj = strings.Replace(cnpj, "-", "", -1)
	cnpj = strings.Replace(cnpj, "/", "", -1)
	if len(cnpj) != 14 {
		return false
	}
	algs := []int{5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2}
	var algProdCpfDig1 = make([]int, 12, 12)
	for key, val := range algs {
		intParsed, _ := strconv.Atoi(string(cnpj[key]))
		sumTmp := val * intParsed
		algProdCpfDig1[key] = sumTmp
	}
	sum := 0
	for _, val := range algProdCpfDig1 {
		sum += val
	}
	digit1 := sum % 11
	if digit1 < 2 {
		digit1 = 0
	} else {
		digit1 = 11 - digit1
	}
	char12, _ := strconv.Atoi(string(cnpj[12]))
	if char12 != digit1 {
		return false
	}
	algs = append([]int{6}, algs...)
	var algProdCpfDig2 = make([]int, 13, 13)
	for key, val := range algs {
		intParsed, _ := strconv.Atoi(string(cnpj[key]))
		sumTmp := val * intParsed
		algProdCpfDig2[key] = sumTmp
	}
	sum = 0
	for _, val := range algProdCpfDig2 {
		sum += val
	}
	digit2 := sum % 11
	if digit2 < 2 {
		digit2 = 0
	} else {
		digit2 = 11 - digit2
	}
	char13, _ := strconv.Atoi(string(cnpj[13]))
	if char13 != digit2 {
		return false
	}
	return true
}

//Valida o CPF recebido
func validarCPF(cpf string) bool {
	cpf = strings.Replace(cpf, ".", "", -1)
	cpf = strings.Replace(cpf, "-", "", -1)
	if len(cpf) != 11 {
		return false
	}
	var eq bool
	var dig string
	for _, val := range cpf {
		if len(dig) == 0 {
			dig = string(val)
		}
		if string(val) == dig {
			eq = true
			continue
		}
		eq = false
		break
	}
	if eq {
		return false
	}
	i := 10
	sum := 0
	for index := 0; index < len(cpf)-2; index++ {
		pos, _ := strconv.Atoi(string(cpf[index]))
		sum += pos * i
		i--
	}
	prod := sum * 10
	mod := prod % 11
	if mod == 10 {
		mod = 0
	}
	digit1, _ := strconv.Atoi(string(cpf[9]))
	if mod != digit1 {
		return false
	}
	i = 11
	sum = 0
	for index := 0; index < len(cpf)-1; index++ {
		pos, _ := strconv.Atoi(string(cpf[index]))
		sum += pos * i
		i--
	}
	prod = sum * 10
	mod = prod % 11
	if mod == 10 {
		mod = 0
	}
	digit2, _ := strconv.Atoi(string(cpf[10]))
	if mod != digit2 {
		return false
	}
	return true
}
