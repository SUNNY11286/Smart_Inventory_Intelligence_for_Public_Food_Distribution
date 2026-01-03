# Smart Inventory Intelligence for Public Food Distribution

**AI-for-Good** solution built on Snowflake to prevent stock-outs of essential food items
by enabling early detection and intelligent redistribution.

## 🧠 Problem
Public food distribution systems often detect shortages too late.
Inventory data is fragmented, and there is no guidance on how to act
when stock-outs occur.

## 💡 Solution
A Snowflake-native inventory intelligence system that:
- Monitors daily inventory and consumption
- Detects early stock-out risks
- Recommends nearest available storage units
- Provides explainable, action-ready insights

## 🏗️ Architecture
- Snowflake SQL & Worksheets
- Dynamic Tables
- Streams & Tasks
- Streamlit Dashboard

## 🚀 Key Features
- Inventory Health Heatmap
- Early Stock-Out Detection
- Distance-Based Redistribution Recommendations
- Transparent “No Source Available” Handling
- Auto-refreshing Streamlit Dashboard

## 🌍 AI for Good Impact
- Reduces food wastage
- Prevents supply disruptions
- Enables fair and timely distribution
- Supports NGOs and public agencies

## ⚙️ Setup (High-Level)
1. Run SQL scripts in `/sql` in order
2. Deploy Streamlit app using Snowflake Streamlit
3. Enable Tasks and Streams

## 📜 License
MIT License
