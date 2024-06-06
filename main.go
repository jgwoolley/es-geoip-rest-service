package main

// FROM: https://blog.maxmind.com/2020/09/enriching-mmdb-files-with-your-own-data-using-go/

import (
	"encoding/csv"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/maxmind/mmdbwriter"
	"github.com/maxmind/mmdbwriter/inserter"
	"github.com/maxmind/mmdbwriter/mmdbtype"
)

type CountryLocale struct {
	geoname_id           int
	locale_code          mmdbtype.String
	continent_code       mmdbtype.String
	continent_names      mmdbtype.Map
	country_iso_code     mmdbtype.String
	country_names        mmdbtype.Map
	is_in_european_union int
}

type ContinentCode struct {
	continentCode mmdbtype.String
	geoname_id    mmdbtype.Int32
	continentName mmdbtype.String
}

var continentCodes = []ContinentCode{
	{
		continentCode: "AF",
		geoname_id:    6255146,
		continentName: "Africa",
	},
	{
		continentCode: "AN",
		geoname_id:    6255152,
		continentName: "Antarctica",
	},
	{
		continentCode: "AS",
		geoname_id:    6255147,
		continentName: "Asia",
	},
	{
		continentCode: "NA",
		geoname_id:    6255149,
		continentName: "North America",
	},
	{
		continentCode: "EU",
		geoname_id:    6255148,
		continentName: "Europe",
	},
	{
		continentCode: "OC",
		geoname_id:    6255151,
		continentName: "Oceania",
	},
	{
		continentCode: "OC",
		geoname_id:    6255151,
		continentName: "Oceania",
	},
	{
		continentCode: "SA",
		geoname_id:    6255150,
		continentName: "South America",
	},
}

var continentCodeLut = createContinentCodeLut()

func createContinentCodeLut() map[mmdbtype.String]ContinentCode {
	continentCodeLut := make(map[mmdbtype.String]ContinentCode)
	for _, continentInfo := range continentCodes {
		continentCodeLut[continentInfo.continentCode] = continentInfo
	}

	return continentCodeLut
}

func convertCountriesFile(localeLut map[int]CountryLocale, writer *mmdbwriter.Tree, inputPath string) {
	f, err := os.Open(inputPath)
	if err != nil {
		log.Fatal("Unable to read input file \""+inputPath+"\": ", err)
	}
	defer f.Close()

	csvReader := csv.NewReader(f)
	_, err = csvReader.Read()
	if err != nil {
		log.Fatal(err)
	}

	count := 0

	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err) // or handle it another way
		}

		network := record[0]
		count += 1

		// Define and insert the new data.
		_, sreNet, err := net.ParseCIDR(network)
		if err != nil {
			log.Fatal(err)
		}

		geoname_id, err := strconv.Atoi(record[1])
		if err != nil {
			geoname_id = -1
		}

		registered_country_geoname_id, err := strconv.Atoi(record[2])
		if err != nil {
			registered_country_geoname_id = -1
		}

		continent_code := localeLut[geoname_id].continent_code
		sreData := mmdbtype.Map{
			"continent": mmdbtype.Map{
				"geoname_id": continentCodeLut[continent_code].geoname_id,
				"code":       continent_code,
				"names":      localeLut[geoname_id].continent_names,
			},
			"country": mmdbtype.Map{
				"geoname_id": mmdbtype.Int32(geoname_id),
				"iso_code":   localeLut[geoname_id].country_iso_code,
				"names":      localeLut[geoname_id].country_names,
			},
			"registered_country": mmdbtype.Map{
				"geoname_id": mmdbtype.Int32(registered_country_geoname_id),
				"iso_code":   localeLut[registered_country_geoname_id].country_iso_code,
				"names":      localeLut[registered_country_geoname_id].country_names,
			},
		}

		if err := writer.InsertFunc(sreNet, inserter.TopLevelMergeWith(sreData)); err != nil {
			log.Fatal(err)
		}
	}
	log.Print("Read " + inputPath + ". " + strconv.Itoa(count) + " value(s).")
}

func convertCountriesLocale(localeLut map[int]CountryLocale, localPath string) {
	f, err := os.Open(localPath)
	if err != nil {
		log.Fatal("Unable to read input file \""+localPath+"\": ", err)
	}
	defer f.Close()

	csvReader := csv.NewReader(f)
	_, err = csvReader.Read()
	if err != nil {
		log.Fatal(err)
	}

	count := 0
	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err) // or handle it another way
		}

		count += 1

		geoname_id, err := strconv.Atoi(record[0])
		if err != nil {
			geoname_id = -1
		}

		is_in_european_union, err := strconv.Atoi(record[6])
		if err != nil {
			geoname_id = -1
		}

		locale_code := mmdbtype.String(record[1])
		continent_code := record[2]
		continent_name := record[3]
		country_iso_code := record[4]
		country_name := record[5]

		localeLuv, ok := localeLut[geoname_id]
		if !ok {
			localeLuv = CountryLocale{
				geoname_id:           geoname_id,
				locale_code:          locale_code,
				continent_code:       mmdbtype.String(continent_code),
				continent_names:      make(mmdbtype.Map),
				country_iso_code:     mmdbtype.String(country_iso_code),
				country_names:        make(mmdbtype.Map),
				is_in_european_union: is_in_european_union,
			}
			localeLut[geoname_id] = localeLuv
		}

		localeLuv.continent_names[locale_code] = mmdbtype.String(continent_name)
		localeLuv.country_names[locale_code] = mmdbtype.String(country_name)
	}
}

func convertCountries(inputPath string, outputPath string) {
	entries, err := os.ReadDir(inputPath)
	if err != nil {
		log.Fatal(err)
	}

	localeLut := make(map[int]CountryLocale)

	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".csv") || strings.HasSuffix(name, "-IPv4.csv") || strings.HasSuffix(name, "-IPv6.csv") {
			continue
		}

		localPath := filepath.Join(inputPath, name)
		convertCountriesLocale(localeLut, localPath)
	}

	// Load the database we wish to enrich.
	writer, err := mmdbwriter.New(mmdbwriter.Options{})
	if err != nil {
		log.Fatal(err)
	}

	ipv4Path := filepath.Join(inputPath, "GeoLite2-Country-Blocks-IPv4.csv")
	convertCountriesFile(localeLut, writer, ipv4Path)

	ipv6Path := filepath.Join(inputPath, "GeoLite2-Country-Blocks-IPv6.csv")
	convertCountriesFile(localeLut, writer, ipv6Path)

	// Write the newly enriched DB to the filesystem.
	fh, err := os.Create(outputPath)
	if err != nil {
		log.Fatal(err)
	}

	_, err = writer.WriteTo(fh)
	if err != nil {
		log.Fatal(err)
	}

	log.Print("Wrote " + outputPath)
}

type CityLocale struct {
	geoname_id             int
	continent_code         mmdbtype.String
	continent_names        mmdbtype.Map
	country_iso_code       mmdbtype.String
	country_names          mmdbtype.Map
	subdivision_1_iso_code mmdbtype.String
	subdivision_1_names    mmdbtype.Map
	subdivision_2_iso_code mmdbtype.String
	subdivision_2_names    mmdbtype.Map
	city_names             mmdbtype.Map
	metro_code             mmdbtype.String
	time_zone              mmdbtype.String
	is_in_european_union   mmdbtype.Int32
}

func convertCitiesLocale(localeLut map[int]CityLocale, localPath string) {
	f, err := os.Open(localPath)
	if err != nil {
		log.Fatal("Unable to read input file \""+localPath+"\": ", err)
	}
	defer f.Close()

	csvReader := csv.NewReader(f)
	_, err = csvReader.Read()
	if err != nil {
		log.Fatal("Could not parse :\""+localPath+"\"", err)
	}

	count := 0
	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatal("Could not parse :\""+localPath+"\"", err)
		}

		if len(record) < 13 {
			log.Fatal("Could not parse :\""+localPath+"\", Record not long enough ", err)
		}

		count += 1

		geoname_id, err := strconv.Atoi(record[0])
		if err != nil {
			geoname_id = -1
		}
		is_in_european_union, err := strconv.Atoi(record[13])
		if err != nil {
			is_in_european_union = -1
		}

		locale_code := mmdbtype.String(record[1])
		continent_code := record[2]
		continent_name := record[3]
		country_iso_code := record[4]
		country_name := record[5]
		subdivision_1_iso_code := record[6]
		subdivision_1_name := record[7]
		subdivision_2_iso_code := record[8]
		subdivision_2_name := record[9]
		city_name := record[10]
		metro_code := record[11]
		time_zone := record[12]

		localeLuv, ok := localeLut[geoname_id]
		if !ok {
			localeLuv = CityLocale{
				geoname_id:             geoname_id,
				continent_code:         mmdbtype.String(continent_code),
				continent_names:        make(mmdbtype.Map),
				country_iso_code:       mmdbtype.String(country_iso_code),
				country_names:          make(mmdbtype.Map),
				subdivision_1_iso_code: mmdbtype.String(subdivision_1_iso_code),
				subdivision_1_names:    make(mmdbtype.Map),
				subdivision_2_iso_code: mmdbtype.String(subdivision_2_iso_code),
				subdivision_2_names:    make(mmdbtype.Map),
				city_names:             make(mmdbtype.Map),
				metro_code:             mmdbtype.String(metro_code),
				time_zone:              mmdbtype.String(time_zone),
				is_in_european_union:   mmdbtype.Int32(is_in_european_union),
			}
			localeLut[geoname_id] = localeLuv
		}

		localeLuv.continent_names[locale_code] = mmdbtype.String(continent_name)
		localeLuv.country_names[locale_code] = mmdbtype.String(country_name)
		localeLuv.subdivision_1_names[locale_code] = mmdbtype.String(subdivision_1_name)
		localeLuv.subdivision_2_names[locale_code] = mmdbtype.String(subdivision_2_name)
		localeLuv.city_names[locale_code] = mmdbtype.String(city_name)
	}
}

func convertCityFile(localeLut map[int]CityLocale, writer *mmdbwriter.Tree, inputPath string) {
	f, err := os.Open(inputPath)
	if err != nil {
		log.Fatal("Unable to read input file \""+inputPath+"\": ", err)
	}
	defer f.Close()

	csvReader := csv.NewReader(f)
	_, err = csvReader.Read()
	if err != nil {
		log.Fatal(err)
	}

	count := 0

	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err) // or handle it another way
		}

		network := record[0]
		count += 1

		// Define and insert the new data.
		_, sreNet, err := net.ParseCIDR(network)
		if err != nil {
			log.Fatal(err)
		}

		geoname_id, err := strconv.Atoi(record[1])
		if err != nil {
			geoname_id = -1
		}

		registered_country_geoname_id, err := strconv.Atoi(record[2])
		if err != nil {
			registered_country_geoname_id = -1
		}

		continent_code := localeLut[geoname_id].continent_code
		sreData := mmdbtype.Map{
			"city": mmdbtype.Map{
				"geoname_id": mmdbtype.Int32(geoname_id),
				"names":      localeLut[geoname_id].city_names,
			},
			"continent": mmdbtype.Map{
				"geoname_id": continentCodeLut[continent_code].geoname_id,
				"code":       continent_code,
				"names":      localeLut[geoname_id].continent_names,
			},
			"country": mmdbtype.Map{
				"geoname_id": mmdbtype.Int32(-1),
				"iso_code":   localeLut[geoname_id].country_iso_code,
				"names":      localeLut[geoname_id].country_names,
			},
			"location": mmdbtype.Map{
				"accuracy_radius": mmdbtype.Int32(-1),
				"latitude":        mmdbtype.Float32(-1),
				"longitude":       mmdbtype.Float32(-1),
				"metro_code":      mmdbtype.Int32(-1),
				"time_zone":       mmdbtype.String(""),
			},
			"postal": mmdbtype.Map{
				"code": mmdbtype.String(""),
			},
			"registered_country": mmdbtype.Map{
				"geoname_id": mmdbtype.Int32(registered_country_geoname_id),
				"iso_code":   localeLut[registered_country_geoname_id].country_iso_code,
				"names":      localeLut[registered_country_geoname_id].country_names,
			},
			"subdivisions": mmdbtype.Slice{
				mmdbtype.Map{
					"geoname_id": mmdbtype.Int32(-1),
					"iso_code":   mmdbtype.String(""),
					"names":      mmdbtype.Map{},
				},
			},
		}

		if err := writer.InsertFunc(sreNet, inserter.TopLevelMergeWith(sreData)); err != nil {
			log.Fatal(err)
		}
	}
	log.Print("Read " + inputPath + ". " + strconv.Itoa(count) + " value(s).")
}

func convertCities(inputPath string, outputPath string) {
	entries, err := os.ReadDir(inputPath)
	if err != nil {
		log.Fatal(err)
	}

	localeLut := make(map[int]CityLocale)

	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".csv") || strings.HasSuffix(name, "-IPv4.csv") || strings.HasSuffix(name, "-IPv6.csv") {
			continue
		}

		localPath := filepath.Join(inputPath, name)
		convertCitiesLocale(localeLut, localPath)
	}

	// Load the database we wish to enrich.
	writer, err := mmdbwriter.New(mmdbwriter.Options{})
	if err != nil {
		log.Fatal(err)
	}

	ipv4Path := filepath.Join(inputPath, "GeoLite2-City-Blocks-IPv4.csv")
	convertCityFile(localeLut, writer, ipv4Path)

	ipv6Path := filepath.Join(inputPath, "GeoLite2-City-Blocks-IPv6.csv")
	convertCityFile(localeLut, writer, ipv6Path)

	// Write the newly enriched DB to the filesystem.
	fh, err := os.Create(outputPath)
	if err != nil {
		log.Fatal(err)
	}

	_, err = writer.WriteTo(fh)
	if err != nil {
		log.Fatal(err)
	}

	log.Print("Wrote " + outputPath)
}

func convertASNFile(writer *mmdbwriter.Tree, inputPath string) {
	f, err := os.Open(inputPath)
	if err != nil {
		log.Fatal("Unable to read input file \""+inputPath+"\": ", err)
	}
	defer f.Close()

	csvReader := csv.NewReader(f)
	_, err = csvReader.Read()
	if err != nil {
		log.Fatal(err)
	}

	count := 0

	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err) // or handle it another way
		}

		network := record[0]
		count += 1

		// Define and insert the new data.
		_, sreNet, err := net.ParseCIDR(network)
		if err != nil {
			log.Fatal(err)
		}

		sreData := mmdbtype.Map{
			"autonomous_system_number":       mmdbtype.String(record[1]),
			"autonomous_system_organization": mmdbtype.String(record[2]),
		}

		if err := writer.InsertFunc(sreNet, inserter.TopLevelMergeWith(sreData)); err != nil {
			log.Fatal(err)
		}
	}
	log.Print("Read " + inputPath + ". " + strconv.Itoa(count) + " value(s).")
}

func convertASN(inputPath string, outputPath string) {
	// Load the database we wish to enrich.
	writer, err := mmdbwriter.New(mmdbwriter.Options{})
	if err != nil {
		log.Fatal(err)
	}

	ipv4Path := filepath.Join(inputPath, "GeoLite2-ASN-Blocks-IPv4.csv")
	convertASNFile(writer, ipv4Path)

	ipv6Path := filepath.Join(inputPath, "GeoLite2-ASN-Blocks-IPv6.csv")
	convertASNFile(writer, ipv6Path)

	// Write the newly enriched DB to the filesystem.
	fh, err := os.Create(outputPath)
	if err != nil {
		log.Fatal(err)
	}

	_, err = writer.WriteTo(fh)
	if err != nil {
		log.Fatal(err)
	}

	log.Print("Wrote " + outputPath)
}

func main() {
	convertASN("./input/GeoLite2-ASN/", "./output/GeoLite2-ASN.mmdb")
	convertCountries("./input/GeoLite2-Country/", "./output/GeoLite2-Country.mmdb")
	convertCities("./input/GeoLite2-City/", "./output/GeoLite2-City.mmdb")
}
