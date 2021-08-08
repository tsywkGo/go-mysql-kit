package canal

import "github.com/pingcap/parser/ast"

const (
	RenameDDL   = "RENAME"
	AlterDDL    = "ALTER"
	DropDDL     = "DROP"
	CreateDDL   = "CREATE"
	TruncateDDL = "TRUNCATE"
)

type node struct {
	db    string
	table string
	ttype string
}

func parseStmt(stmt ast.StmtNode) (ns []*node) {
	switch t := stmt.(type) {
	case *ast.RenameTableStmt:
		for _, tableInfo := range t.TableToTables {
			n := &node{
				db:    tableInfo.OldTable.Schema.String(),
				table: tableInfo.OldTable.Name.String(),
				ttype: RenameDDL,
			}
			ns = append(ns, n)
		}
	case *ast.AlterTableStmt:
		n := &node{
			db:    t.Table.Schema.String(),
			table: t.Table.Name.String(),
			ttype: AlterDDL,
		}
		ns = []*node{n}
	case *ast.DropTableStmt:
		for _, table := range t.Tables {
			n := &node{
				db:    table.Schema.String(),
				table: table.Name.String(),
				ttype: DropDDL,
			}
			ns = append(ns, n)
		}
	case *ast.CreateTableStmt:
		n := &node{
			db:    t.Table.Schema.String(),
			table: t.Table.Name.String(),
			ttype: CreateDDL,
		}
		ns = []*node{n}
	case *ast.TruncateTableStmt:
		n := &node{
			db:    t.Table.Schema.String(),
			table: t.Table.Schema.String(),
			ttype: TruncateDDL,
		}
		ns = []*node{n}
	}
	return
}
