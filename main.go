package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

type StationLocation struct {
	name      string
	latitude  float64
	longitude float64
}

type StationDistance struct {
	firstStation  string
	secondStation string
	distance      float64
}

func DegreeToRad(degree float64) float64 {
	return degree * (math.Pi / 180)
}

func (station *StationLocation) GetDistance(other *StationLocation) float64 {
	latitudeDistance := DegreeToRad(other.latitude - station.latitude)
	longitudeDistance := DegreeToRad(other.longitude - station.longitude)

	a := math.Sin(latitudeDistance/2)*math.Sin(latitudeDistance/2) +
		math.Cos(DegreeToRad(station.latitude))*math.Cos(DegreeToRad(other.latitude))*
			math.Sin(longitudeDistance/2)*math.Sin(longitudeDistance/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

	earthRadius := 6371.0
	distance := earthRadius * c
	return distance
}

func StreamCSV() (<-chan []string, <-chan error) {
	records := make(chan []string)
	errs := make(chan error)

	go func() {
		defer close(records)
		defer close(errs)

		f, err := os.Open("locations.csv")
		if err != nil {
			errs <- err
			return
		}
		defer f.Close()

		r := csv.NewReader(f)
		for {
			rec, err := r.Read()
			if err == io.EOF {
				return
			}
			if err != nil {
				errs <- err
				return
			}
			records <- rec
		}
	}()

	return records, errs
}

func GetLocation(record []string) (*StationLocation, error) {
	name := strings.TrimSpace(record[0])
	latitude, err := strconv.ParseFloat(strings.TrimSpace(record[1]), 64)
	if err != nil {
		return nil, err
	}
	longitude, err := strconv.ParseFloat(strings.TrimSpace(record[2]), 64)
	if err != nil {
		return nil, err
	}
	return &StationLocation{
		name:      name,
		latitude:  latitude,
		longitude: longitude,
	}, nil
}

func main() {
	start := time.Now()
	recordChannel, errorChannel := StreamCSV()
	counter := 0
	var locations []*StationLocation
	headers := <-recordChannel
	fmt.Println("Headers:", headers)
	for record := range recordChannel {
		location, err := GetLocation(record)
		if err != nil {
			log.Fatal(err)
		}
		locations = append(locations, location)
		counter++
	}
	if err := <-errorChannel; err != nil {
		log.Fatalf("stream error: %v", err)
	}
	log.Printf("%d records received", counter)

	var distances []*StationDistance
	for i := range len(locations) {
		firstStation := locations[i]
		for j := i + 1; j < len(locations); j++ {
			secondStation := locations[j]
			distances = append(distances, &StationDistance{
				firstStation:  firstStation.name,
				secondStation: secondStation.name,
				distance:      firstStation.GetDistance(secondStation),
			})
		}
	}
	//for _, entry := range distances {
	//	fmt.Printf("%s\t%s\t%f\n", entry.firstStation, entry.secondStation, entry.distance)
	//}
	totalTime := time.Since(start)
	fmt.Printf("We did %v calculations in %v seconds!\n", len(distances), totalTime.Seconds())
}
