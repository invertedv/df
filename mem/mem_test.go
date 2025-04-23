package df

import (
	"fmt"
	"os"

	d "github.com/invertedv/df"
)

// Examples

func ExampleFileLoad() {
	var (
		f *d.Files
		e error
	)
	if f, e = d.NewFiles(); e != nil {
		panic(e)
	}

	fileToOpen := os.Getenv("datapath") + "d1.csv"
	if ex := f.Open(fileToOpen); ex != nil {
		panic(ex)
	}

	var (
		df *DF
		e2 error
	)
	if df, e2 = FileLoad(f); e2 != nil {
		panic(e2)
	}

	fmt.Println(df.RowCount())
	// Output:
	// 6
}
