# Can I Fish This? - BC Freshwater Fishing Regulations

Interactive web application for exploring BC freshwater fishing regulations.

**Live Site:** [canifishthis.ca](https://canifishthis.ca)

## Features

- 🗺️ Interactive map of BC streams, lakes, and waterbodies
- 🔍 Search for specific water bodies by name
- 📍 Click waterbodies to view fishing regulations
- 🎣 See bag limits, species restrictions, and seasonal closures
- 📱 Responsive design for mobile and desktop

## Tech Stack

- **React** - UI framework
- **TypeScript** - Type safety
- **MapLibre GL JS** - Map rendering
- **PMTiles** - Efficient tile format for regulation data
- **Vite** - Build tool
- **Fuse.js** - Fuzzy search for waterbody names

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
