package main

import (
	"encoding/csv"
	"fmt"
	"os"
)

// CSV fields
const (
	Dir  = 0 // Direction
	Time = 1 // Timestamp
	Rate = 2 // Rate
)

func writeCSV(path string, info *ExportInfo) error {

	out, errCreate := os.Create(path)
	if errCreate != nil {
		return errCreate
	}

	w := csv.NewWriter(out)

	if info.completionTime == 0 {
		entry := []string{"DIRECTION", "TIME", "RATE"}

		if errHeader := w.Write(entry); errHeader != nil {
			return errHeader
		}

		entry[Dir] = "input"
		for i, x := range info.Input.XValues {
			entry[Time] = x.String()
			entry[Rate] = fmt.Sprintf("%v", info.Input.YValues[i])
			if err := w.Write(entry); err != nil {
				return err
			}
		}

		entry[Dir] = "output"
		for i, x := range info.Output.XValues {
			entry[Time] = x.String()
			entry[Rate] = fmt.Sprintf("%v", info.Output.YValues[i])
			if err := w.Write(entry); err != nil {
				return err
			}
		}
	} else {
		entry := []string{"completion time"}
		if errHeader := w.Write(entry); errHeader != nil {
			return errHeader
		}
		entry[0] = fmt.Sprintf("%v\n", info.completionTime.Nanoseconds())
		if err := w.Write(entry); err != nil {
			return err
		}
	}

	w.Flush()

	return out.Close()
}

func appendCSV(path string, info *ExportInfo) error {

	out, errCreate := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0644)
	if errCreate != nil {
		return errCreate
	}

	w := csv.NewWriter(out)

	if info.completionTime == 0 {
		entry := []string{"DIRECTION", "TIME", "RATE"}

		if errHeader := w.Write(entry); errHeader != nil {
			return errHeader
		}

		entry[Dir] = "input"
		for i, x := range info.Input.XValues {
			entry[Time] = x.String()
			entry[Rate] = fmt.Sprintf("%v", info.Input.YValues[i])
			if err := w.Write(entry); err != nil {
				return err
			}
		}

		entry[Dir] = "output"
		for i, x := range info.Output.XValues {
			entry[Time] = x.String()
			entry[Rate] = fmt.Sprintf("%v", info.Output.YValues[i])
			if err := w.Write(entry); err != nil {
				return err
			}
		}
	} else {
		entry := []string{
				fmt.Sprintf("%v", info.completionTime.Nanoseconds())}
		if err := w.Write(entry); err != nil {
			return err
		}
	}

	w.Flush()

	return out.Close()
}

func initCSV(path string) (error) {

	out, errCreate := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if errCreate != nil {
		return errCreate
	}

	return out.Close()
}

func openCSV(path string, header []string) (*csv.Writer, *os.File, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, nil, err
	}
	w := csv.NewWriter(file)

	// fill the first row as header
	if errHeader := w.Write(header); errHeader != nil {
		return nil, nil, errHeader
	}

	return w, file, nil
}

func closeCSV(w *csv.Writer, f *os.File) error {
	w.Flush()
	return f.Close()
}
