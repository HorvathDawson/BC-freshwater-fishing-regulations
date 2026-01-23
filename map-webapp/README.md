# BC Map Web App

A simple, clean web application for displaying BC map data using MapLibre GL JS and PMTiles.

## Features

- Interactive map with BC PMTiles base layer
- MapLibre GL JS for rendering
- Navigation controls
- Scale control
- Responsive design

## Tech Stack

- **React** - UI framework
- **TypeScript** - Type safety
- **MapLibre GL JS** - Map rendering
- **PMTiles** - Efficient tile format
- **Vite** - Build tool

## Getting Started

### Install Dependencies

```bash
npm install
```

### Development

```bash
npm run dev
```

### Build for Production

```bash
npm run build
```

### Preview Production Build

```bash
npm run preview
```

## Project Structure

```
map-webapp/
├── src/
│   ├── components/
│   │   └── Map.tsx          # Map component
│   ├── App.tsx              # Main app component
│   ├── App.css              # App styles
│   ├── index.css            # Global styles
│   └── main.tsx             # Entry point
├── public/
│   └── data/
│       └── bc.pmtiles       # BC map tiles
└── package.json
```

## Map Component

The Map component is a clean, reusable React component that:
- Initializes MapLibre with PMTiles protocol
- Loads the BC PMTiles base layer
- Adds navigation and scale controls
- Handles proper cleanup on unmount

## Customization

To modify the map layers, edit [src/components/Map.tsx](src/components/Map.tsx) and update the `style.layers` array with your desired layer configuration.
