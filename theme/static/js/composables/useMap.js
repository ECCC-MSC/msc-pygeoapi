import 'leaflet-non-esm'
import 'leaflet.markercluster'
import { ref, computed, watch, onMounted, onBeforeMount } from 'vue'

export default function useMap(mapElemId, geoJsonData, itemsPath, tileLayerUrl, tileLayerAttr, bboxPermalink, locale) {
  let map, layerItems, markers, pointIntersectedLayers, popupDisplayText, layerIdPopup, clickPopup
  const maxZoom = 15
  const bbox = ref('')
  // Leaflet LatLngBounds simple array format
  const bboxLatLngBounds = computed(() => {
    const bboxSplit = bbox.value.split(',')
    if (bboxSplit.length < 4) {
      return [[-85, -175], [85, 175]]
    } else {
      return [
        [bboxSplit[1], bboxSplit[0]],
        [bboxSplit[3], bboxSplit[2]]
      ]
    }
  })
  const geoJsonIsPointGeometry = computed(() => {
    if (Object.prototype.hasOwnProperty.call(geoJsonData.value, 'features')) {
      if (geoJsonData.value.features.length > 0 ) {
        if ([undefined, null].includes(geoJsonData.value.features[0]['geometry'])) {
          return false
        }
        if (geoJsonData.value.features[0]['geometry']['type'] === 'Point') {
          return true
        }
      }
    }
    return false
  })
  const geoJsonIsPolygonGeometry = computed(() => {
    if (Object.prototype.hasOwnProperty.call(geoJsonData.value, 'features')) {
      if (geoJsonData.value.features.length > 0 ) {
        const polygon_types = ['Polygon', 'MultiPolygon']
        for (const feature_geometry of geoJsonData.value.features) {
          if (![undefined, null].includes(feature_geometry['geometry'])) {
            if (polygon_types.includes(feature_geometry['geometry']['type'])) {
              return true
            }
          }
        }
      }
    }
    return false
  })
  const updateBboxEvent = function(evt) {
    const bboxString = evt.sourceTarget.getBounds().toBBoxString()
    const bboxSplit = bboxString.split(',').map((x, i) => {
      // ensure bounds don't extend past -180,-90,180,90
      let coord = parseFloat(x).toFixed(2)
      if (i % 2 === 0) { // lon
        if (coord > 180) {
          coord = 180
        } else if (coord < -180) {
          coord = -180
        }
      } else { // lat
        if (coord > 90) {
          coord = 90
        } else if (coord < -90) {
          coord = -90
        }
      }
      return coord
    })
    bbox.value = bboxSplit.join(',')
  }

  const updatePopup = function(evt) {
    if (this.feature === undefined) {
      return
    }

    let selectedLayer = this.feature.id
    let intersectingPolygons = []

    let latlng = map.mouseEventToLatLng(evt.originalEvent)
    let [x, y] = [latlng.lng, latlng.lat]

    // Gather IDs of layers that contain the coord
    layerItems.eachLayer(function (layer) {
      let inside = false
      let coordinates = layer.feature.geometry.coordinates
      if (layer.feature.id === selectedLayer) {
        inside = true
      } else if (layer.feature.geometry.type === 'MultiPolygon') {
        for (const coordSet of coordinates) {
          let currentPolygon = coordSet[0]
          inside = checkIfInside(currentPolygon, x, y)
          if (inside) {
            break
          }
        }
      } else {
        let currentPolygon = coordinates[0]
        inside = checkIfInside(currentPolygon, x, y)
      }

      if (inside) {
        // Add the ID to the list of IDs to display in the popup
        intersectingPolygons.push(layer.feature.id)
      }
    })
    pointIntersectedLayers = intersectingPolygons

    layerItems.eachLayer(function (layer) {
      if (layer.feature.id === selectedLayer) {

        // List element for the popup
        popupDisplayText = document.createElement('ul')

        // Each list item will be a link for an intersecting layer
        pointIntersectedLayers.forEach(layerId => {
          const url = itemsPath + '/' + layerId + `?lang=${locale}`
          const anchorLink = document.createElement('a')
          anchorLink.href = url
          anchorLink.textContent = layerId
          const li = document.createElement('li')
          li.appendChild(anchorLink)
          popupDisplayText.appendChild(li)
        })

        // Ensures each layer ID is spaced out
        layerIdPopup = popupDisplayText.querySelectorAll('li:not(:last-child)')
        layerIdPopup.forEach(li => {
          li.classList.add('popup-items-text')
        })

        // Prevents the popup from displaying bullet points and
        // adds a scroll bar when many layers intersect the point
        popupDisplayText.classList.add('popup-items-list')

        // Handles the highlighting of layers and resetting them back to normal
        popupDisplayText.addEventListener('mouseover', hoverOver)
        popupDisplayText.addEventListener('mouseout', hoverOff)

        clickPopup = L.popup(L.latLng(y, x), {content: popupDisplayText})
        clickPopup.openOn(map)

        clickPopup.on('remove', function () {
          popupDisplayText.removeEventListener('mouseover', hoverOver)
          popupDisplayText.removeEventListener('mouseout', hoverOff)
          popupDisplayText = null
        })
      }
    })
  }

  const checkIfInside = function (currentPolygon, x, y) {
    let result = false
    let currPolyLen = currentPolygon.length

    // Ray-Casting algorithm
    for (let i=0, j=currPolyLen - 1; i < currPolyLen; j = i++) {
      const [xi, yi] = currentPolygon[i]
      const [xj, yj] = currentPolygon[j]
      const intersect = ((yi > y) !== (yj > y)) &&
        (x < (xj - xi) * (y - yi) / ((yj - yi) || 1e-16) + xi)
      if (intersect) {
        result = !result
      }
    }
    return result
  }

  // map initialize
  const setupMap = function() {
    map = L.map(mapElemId, {
      zoomDelta: 0.25,
      zoomSnap: 0.25
    }).setView([45, -75], 5)
    map.addLayer(new L.TileLayer(
      tileLayerUrl, {
        minZoom: 2,
        maxZoom: maxZoom,
        attribution: tileLayerAttr
      }
    ))
    layerItems = new L.GeoJSON({type: 'FeatureCollection', features: []})
    // Initialize MarkerClusterGroup
    markers = L.markerClusterGroup({
      disableClusteringAtZoom: 9,
      chunkedLoading: true,
      chunkInterval: 500,
    })
    map.addLayer(markers)

    if (geoJsonIsPointGeometry.value) {
      markers.clearLayers().addLayer(layerItems)
    } else {
      // Initialize without MarkerClusterGroup
      map.addLayer(layerItems)
    }

    // update bbox for permalink feature
    map.on('moveend', updateBboxEvent)
  }
  onMounted(setupMap)

  // update map with new geoJson data
  watch(geoJsonData, (newData, oldData) => {
    map.closePopup()
    if (map.hasLayer(layerItems) && geoJsonIsPointGeometry.value === false) {
      map.removeLayer(layerItems)
    }

    // feature collection
    if (Object.prototype.hasOwnProperty.call(geoJsonData.value, 'features')) {
      if (geoJsonData.value.features.length === 0) {
        markers.clearLayers();
        return false
      }
    } else if (geoJsonData.value.type === 'Feature') { // single feature
      if (geoJsonData.value.geometry === null) {
        map.setView([0, 0], 1)
        return false
      }
    }

    // To check if calling fitBounds is needed
    let fitBbox = false

    for (const feat of geoJsonData.value.features) {
      if (feat.geometry !== null) {
        fitBbox = true
        break
      }
    }

    layerItems = new L.GeoJSON(geoJsonData.value, {
      onEachFeature: function (feature, layer) {
        let url = itemsPath + '/' + feature.id + `?lang=${locale}`
        let html = '<span><a href="' + url + '">' + feature.id + '</a></span>'
        layer.bindPopup(html)
        if (geoJsonIsPolygonGeometry.value) {
          layer.on('click', updatePopup)
        }
      }
    })

    // Add layer back depending on point vs other geometry
    if (geoJsonIsPointGeometry.value) {
      markers.clearLayers().addLayer(layerItems) // with MarkerClusterGroup
    } else {
      map.addLayer(layerItems) // without MarkerClusterGroup
    }

    if (fitBbox) {
      if (bbox.value === '' || !bboxPermalink.value) { // no bbox
        map.fitBounds(layerItems.getBounds(), {maxZoom: 5})
      } else if (Object.keys(oldData).length === 0) { // first time load
        // don't trigger moveend event after a fitBounds() to avoid overwriting original BBOX from permalink
        map.off('moveend', updateBboxEvent) // temporarily remove moveend event
        map.fitBounds(bboxLatLngBounds.value, {maxZoom: maxZoom})
        setTimeout(() => { // add moveend event back after short delay
          map.on('moveend', updateBboxEvent)
        }, 2000)
      }
    }
  })

  const hoverOver = function(evt) {
    evt.target.style.color = '#0535d2'

    const highlightStyle = {
      color: "#ff7800",
      weight: 5,
      fillOpacity: 0.5
    }
    const lessVisibleStyle = {
      weight: 0,
      fillOpacity: 0,
      opacity: 0
    }

    layerItems.eachLayer(function (layer) {
      if (layer.feature.id === evt.target.textContent) {
        layer.setStyle(highlightStyle)
        layer.bringToFront()
      } else {
        // make the other layers invisible
        layer.setStyle(lessVisibleStyle)
      }
    })
  }

  const hoverOff = function(evt) {
    evt.target.style.color = '#0078A8'
    const defaultStyle = {
      color: "#3388ff",
      weight: 3,
      fillOpacity: 0.2,
      opacity: 1
    }
    layerItems.eachLayer(function (layer) {
      layer.setStyle(defaultStyle)
    })
  }

  return {
    setupMap, bbox
  }
}