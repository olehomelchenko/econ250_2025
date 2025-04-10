# Integrated Sales Data Model (int_fp_sales_full)

## Overview
This model serves as the foundation for all order analytics by combining scattered order information into a single denormalized table at the order level (`order_id`). It integrates data from 7 source tables while preserving the one-to-many relationships through BigQuery's nested structures.

## Design Decisions

### Data Structure Strategy
1. **Denormalization Approach**:
   - Combined all order-related entities into a single wide table
   - Preserved relational integrity through nested structures 

2. **Nested Data Handling**:
   ```sql
   ARRAY_AGG(STRUCT(...)) AS payments  -- All payment methods preserved
   ARRAY_AGG(STRUCT(...)) AS items     -- Full item details with product/seller info

- Payments and items stored as arrays of structs to maintain multiple records per order

- Avoids data duplication while keeping all related information accessible


**Aggregated Metrics**:

`total_payment_value`: Sum of all payments for quick analysis

`item_count`: Total products per order

`total_order_value`: Sum of (price + freight) for revenue calculations

