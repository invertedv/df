package df

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	grob "github.com/MetalBlueberry/go-plotly/graph_objects"
	"github.com/MetalBlueberry/go-plotly/offline"
)

type Plot struct {
	Fig *grob.Fig
	Lay *grob.Layout
}

type Opt func(plot *Plot) *Plot

func NewPlot(opt ...Opt) *Plot {
	fig := &grob.Fig{}
	lay := &grob.Layout{}
	fig.Layout = lay
	p := &Plot{Fig: fig, Lay: lay}
	for _, o := range opt {
		o(p)
	}

	return p
}

func WithWidth(w float64) Opt {
	if w < 0.0 {
		panic(fmt.Errorf("negative width"))
	}
	return func(p *Plot) *Plot {
		p.Lay.Width = w
		return p
	}
}

func WithHeight(h float64) Opt {
	if h < 0.0 {
		panic(fmt.Errorf("negative height"))
	}
	return func(p *Plot) *Plot {
		p.Lay.Height = h
		return p
	}
}

func WithTitle(title string) Opt {
	return func(p *Plot) *Plot { p.Lay.Title = &grob.LayoutTitle{Text: title}; return p }
}

// add to below x title
func WithSubtitle(subTitle string) Opt {
	return func(p *Plot) *Plot {
		if p.Lay.Xaxis == nil {
			p.Lay.Xaxis = &grob.LayoutXaxis{}
		}
		if p.Lay.Xaxis.Title == nil {
			p.Lay.Xaxis.Title = &grob.LayoutXaxisTitle{}
		}

		xAxis := p.Lay.Xaxis
		var xLabel string
		if xLabel = xAxis.Title.Text.(string); xLabel != "" {
			xLabel += "<br>"
		}
		xAxis.Title.Text = xLabel + subTitle
		return p
	}
}

func WithLegend(show bool) Opt {
	return func(p *Plot) *Plot {
		if show {
			p.Lay.Showlegend = grob.True
		} else {
			p.Lay.Showlegend = grob.False
		}

		return p
	}
}

func WithXlabel(label string) Opt {
	return func(p *Plot) *Plot {
		if p.Lay.Xaxis == nil {
			p.Lay.Xaxis = &grob.LayoutXaxis{}
		}

		if p.Lay.Xaxis.Title == nil {
			p.Lay.Xaxis.Title = &grob.LayoutXaxisTitle{}
			p.Lay.Xaxis.Title.Text = ""
		}

		xAxis := p.Lay.Xaxis

		subTitle := ""
		xLabel := xAxis.Title.Text.(string)
		if ind := strings.Index(xLabel, "<br>"); ind > 0 {
			subTitle = xLabel[ind:]
		}

		xAxis.Title.Text = label + subTitle
		return p
	}
}

func WithYlabel(label string) Opt {
	return func(p *Plot) *Plot {
		if p.Lay.Yaxis == nil {
			p.Lay.Yaxis = &grob.LayoutYaxis{}
		}
		if p.Lay.Yaxis.Title == nil {
			p.Lay.Yaxis.Title = &grob.LayoutYaxisTitle{}
		}

		yAxis := p.Lay.Yaxis
		yAxis.Title.Text = label
		return p
	}
}

func (p *Plot) PlotXY(x, y Column, seriesName, color string) error {
	if x.DataType() != DTfloat || y.DataType() != DTfloat {
		return fmt.Errorf("xy plots require floats")
	}

	tr := &grob.Scatter{Name: seriesName, X: x.Data().AsFloat(), Y: y.Data().AsFloat(),
		Mode: grob.ScatterModeLines, Line: &grob.ScatterLine{Color: color}}

	p.Fig.AddTraces(tr)

	return nil
}

func (p *Plot) Show(browser, fileName string) error {
	const nameLength = 8

	if browser == "" {
		browser = "xdg-open"
	}

	tmpFile := false
	if fileName == "" {
		fileName = tempFile("html", nameLength)

		tmpFile = true
		offline.ToHtml(p.Fig, fileName)
	}

	var cmd *exec.Cmd
	if browser == "" {
		cmd = exec.Command("xdg-open", "-url", fileName)
	} else {
		cmd = exec.Command(browser, fileName)
	}

	if e := cmd.Start(); e != nil {
		return e
	}

	time.Sleep(time.Second) // need to pause while browser loads graph

	if tmpFile {
		if e := os.Remove(fileName); e != nil {
			return e
		}
	}

	return nil
}

func (p *Plot) Save(fileName, format string) error {
	return nil
}

// *********** Helpers ***********

// tempFile produces a random temp file name in the system's tmp location.
// The file has extension "ext". The file name begins with "tmp" has length 3 + length.
func tempFile(ext string, length int) string {
	return Slash(os.TempDir()) + "tmp" + RandomLetters(length) + "." + ext
}
