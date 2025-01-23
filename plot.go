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

type PlotOpt func(plot *Plot) error

func NewPlot(opt ...PlotOpt) (*Plot, error) {
	fig := &grob.Fig{}
	lay := &grob.Layout{}
	fig.Layout = lay
	p := &Plot{Fig: fig, Lay: lay}

	for _, o := range opt {
		if e := o(p); e != nil {
			return nil, e
		}
	}

	return p, nil
}

func PlotWidth(w float64) PlotOpt {
	return func(p *Plot) error {
		if w < 25 {
			return fmt.Errorf("invalid width: %v", w)
		}

		p.Lay.Width = w
		return nil
	}
}

func PlotHeight(h float64) PlotOpt {
	return func(p *Plot) error {
		if h <= 25 {
			return fmt.Errorf("invalid height: %v", h)
		}

		p.Lay.Height = h
		return nil
	}
}

func PlotTitle(title string) PlotOpt {
	return func(p *Plot) error {
		p.Lay.Title = &grob.LayoutTitle{Text: title}
		return nil
	}
}

// add to below x title
func PlotSubtitle(subTitle string) PlotOpt {
	return func(p *Plot) error {
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
		return nil
	}
}

func PlotLegend(show bool) PlotOpt {
	return func(p *Plot) error {
		if show {
			p.Lay.Showlegend = grob.True
		} else {
			p.Lay.Showlegend = grob.False
		}

		return nil
	}
}

func PlotXlabel(label string) PlotOpt {
	return func(p *Plot) error {
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

		return nil
	}
}

func PlotYlabel(label string) PlotOpt {
	return func(p *Plot) error {
		if p.Lay.Yaxis == nil {
			p.Lay.Yaxis = &grob.LayoutYaxis{}
		}
		if p.Lay.Yaxis.Title == nil {
			p.Lay.Yaxis.Title = &grob.LayoutYaxisTitle{}
		}

		yAxis := p.Lay.Yaxis
		yAxis.Title.Text = label

		return nil
	}
}

func (p *Plot) PlotXY(x, y Column, seriesName, color string) error {
	if x.DataType() != DTfloat || y.DataType() != DTfloat {
		return fmt.Errorf("xy plots require floats")
	}

	var (
		xv, yv []float64
		e      error
	)
	if xv, e = x.Data().AsFloat(); e != nil {
		return e
	}
	if yv, e = y.Data().AsFloat(); e != nil {
		return e
	}

	tr := &grob.Scatter{Name: seriesName, X: xv, Y: yv,
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
	return slash(os.TempDir()) + "tmp" + RandomLetters(length) + "." + ext
}
