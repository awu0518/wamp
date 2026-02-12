# Project Plan

## Part 1: What We Completed Last Semester

### Project Overview
We built a backend API server for geographic data using Flask and MongoDB.

### What We Built

#### REST API with Full CRUD Operations
- Created about 20 endpoints for managing cities, states, and countries
- Implemented get, create, update, and delete operations for all geographic data
- Added caching for better performance on eligible endpoints
- Built authentication and role-based access control for security

**Requirements Met:** RESTful API design, CRUD functionality, user authentication, authorization

#### Database Integration
- Set up MongoDB with data models for cities, states, and countries
- Added data validation and backup system

**Requirements Met:** Database integration, data persistence

#### Testing and Documentation
- Unit tests for all modules and API endpoints
- API documentation using Flask-RESTX

**Requirements Met:** Automated testing, code documentation

#### Deployment
- GitHub Actions for continuous integration
- Automated deployment to PythonAnywhere

**Requirements Met:** Production deployment, CI/CD pipeline

### Technical Stack
Python, Flask, MongoDB, pytest, PythonAnywhere, GitHub Actions

---

## Part 2: Goals for This Semester

### Project Overview
We are building a frontend web application that combines location tracking with personal journals. Users can mark places they've visited on a map and write about their experiences.

### Main Goals

#### 1. React Frontend with Deployment
Build a React application and deploy it to Vercel with automatic deployment from GitHub.

**Requirement:** Frontend framework implementation and deployment

**How:** Set up React with Vite, organize components clearly, connect GitHub to Vercel for auto-deployment.

#### 2. Testing
Write tests for all React components to make sure everything works.

**Requirement:** Frontend testing

**How:** Use React Testing Library to test user interactions, form submissions, and data display.

#### 3. Interactive Map
Display a map where users can see and add locations they've visited.

**Requirement:** Interactive UI component

**How:** Use Leaflet or Mapbox library, let users click to add places, show markers for all locations.

#### 4. Location Submission Form
Let users add new places with notes about their visit.

**Requirement:** Data input forms

**How:** Create a form with fields for location name, coordinates, visit date, and notes. Connect to backend API.

#### 5. User Account System
Allow people to create accounts and manage their entries.

**Requirement:** User authentication

**How:** Build login and signup pages. Users can only edit or delete their own entries.

#### 6. Personal History Page
Show users all the places they've visited with their notes.

**Requirement:** Data display and filtering

**How:** Create a page listing all locations for the logged-in user with sorting options.

#### 7. Leaderboard
Show which users have visited the most places and which locations are most popular.

**Requirement:** Data aggregation and display

**How:** Build a leaderboard page that queries the backend for visit counts and rankings.

#### 8. About Page
Basic page with information about the application.

**Requirement:** Static content pages

**How:** Create a simple about page explaining what the app does.

### Technical Stack

React, Leaflet or Mapbox, React Testing Library, Vercel, connects to existing Flask backend
