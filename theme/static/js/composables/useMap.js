import 'leaflet-non-esm' 
import 'leaflet.markercluster'
import { ref, computed, watch, onMounted, onBeforeMount } from 'vue'

export default function useMap(mapElemId, geoJsonData, itemsPath, tileLayerUrl, tileLayerAttr, bboxPermalink, locale) {
  let map, layerItems, markers
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
    if (map.hasLayer(layerItems) && geoJsonIsPointGeometry.value === false) {
      map.removeLayer(layerItems)
    }
    
    // feature collection
    if (Object.prototype.hasOwnProperty.call(geoJsonData.value, 'features')) {
      if (geoJsonData.value.features.length === 0 || geoJsonData.value.features[0].geometry === null) {
        markers.clearLayers();
        return false
      }
    } else if (geoJsonData.value.type === 'Feature') { // single feature
      if (geoJsonData.value.geometry === null) {
        map.setView([0, 0], 1)
        return false
      }
    }
    
    layerItems = new L.GeoJSON(geoJsonData.value, {
      onEachFeature: function (feature, layer) {
        let url = itemsPath + '/' + feature.id + `?lang=${locale}`
        let html = '<span><a href="' + url + '">' + feature.id + '</a></span>'
        layer.bindPopup(html)
      }
    })

    // Add layer back depending on point vs other geometry
    if (geoJsonIsPointGeometry.value) {
      markers.clearLayers().addLayer(layerItems) // with MarkerClusterGroup
    } else {
      map.addLayer(layerItems) // without MarkerClusterGroup
    }

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
  })

  return {
    setupMap, bbox
  }
}