package main

import (
	"fmt"
	"log"
	"sort"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func main() {
	// Create DynamoDB session with profile
	sess, err := session.NewSessionWithOptions(session.Options{
		Profile: "cipherowl-aws-dev",
		Config: aws.Config{
			Region: aws.String("us-east-1"),
		},
	})
	if err != nil {
		log.Fatal("Failed to create session:", err)
	}
	
	svc := dynamodb.New(sess)
	
	tableName := "example_chainstorage_events_ethereum_mainnet"
	eventTag := 3
	
	// Test specific heights around problematic range
	testHeights := []int{19793200, 19793210, 19793220, 19793230, 19793231, 19793232, 
	                     19793240, 19793250, 19793260, 19793270, 19793280, 19793290, 
	                     19793300, 19793310, 19793320, 19793330, 19793340, 19793350}
	
	fmt.Printf("Testing DynamoDB table: %s\n", tableName)
	fmt.Printf("Event Tag: %d\n", eventTag)
	fmt.Println("=" + string(make([]byte, 60)))
	
	allEventIds := []int{}
	heightsWithNoEvents := []int{}
	
	for _, height := range testHeights {
		input := &dynamodb.QueryInput{
			TableName: aws.String(tableName),
			KeyConditionExpression: aws.String("event_tag = :tag AND block_height = :height"),
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				":tag": {
					N: aws.String(fmt.Sprintf("%d", eventTag)),
				},
				":height": {
					N: aws.String(fmt.Sprintf("%d", height)),
				},
			},
		}
		
		result, err := svc.Query(input)
		if err != nil {
			fmt.Printf("Height %d: ERROR - %v\n", height, err)
			continue
		}
		
		if *result.Count == 0 {
			fmt.Printf("Height %d: NO EVENTS\n", height)
			heightsWithNoEvents = append(heightsWithNoEvents, height)
		} else {
			eventIds := []int{}
			for _, item := range result.Items {
				if eventIdAttr, ok := item["event_id"]; ok && eventIdAttr.N != nil {
					var eventId int
					fmt.Sscanf(*eventIdAttr.N, "%d", &eventId)
					eventIds = append(eventIds, eventId)
					allEventIds = append(allEventIds, eventId)
				}
			}
			sort.Ints(eventIds)
			fmt.Printf("Height %d: %d events, Event IDs: %v\n", height, *result.Count, eventIds)
			
			// Check for missing events
			for _, id := range eventIds {
				if id >= 19793232 && id <= 19793529 {
					fmt.Printf("  *** FOUND MISSING EVENT %d at height %d ***\n", id, height)
				}
			}
		}
	}
	
	fmt.Println("\n" + "=" + string(make([]byte, 60)))
	fmt.Printf("Summary:\n")
	fmt.Printf("Heights with no events: %v\n", heightsWithNoEvents)
	
	// Sort and check for gaps
	sort.Ints(allEventIds)
	fmt.Printf("\nEvent IDs collected: %d events\n", len(allEventIds))
	if len(allEventIds) > 0 {
		fmt.Printf("Event ID range: %d to %d\n", allEventIds[0], allEventIds[len(allEventIds)-1])
		
		// Check for gaps
		fmt.Println("\nChecking for gaps:")
		for i := 1; i < len(allEventIds); i++ {
			if allEventIds[i] != allEventIds[i-1]+1 {
				gap := allEventIds[i] - allEventIds[i-1] - 1
				fmt.Printf("  GAP: %d -> %d (missing %d events)\n", 
					allEventIds[i-1], allEventIds[i], gap)
			}
		}
	}
}
