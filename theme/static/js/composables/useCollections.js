import { ref, computed } from 'vue'

export default function useCollections() {
  const collectionsJson = ref(JSON_DATA) // global JSON_DATA from jinja rendered JSON
  const collectionsTotal = computed(() => {
    if (Object.prototype.hasOwnProperty.call(collectionsJson.value, 'numberMatched')) {
      return collectionsJson.value.numberMatched
    } else {
      return 0
    }
  })
  const collections = computed(() => {
    if (Object.prototype.hasOwnProperty.call(collectionsJson.value, 'collections')) {
      // check for coverage type in links
      collectionsJson.value.collections.forEach((collection) => {
        if (!Object.prototype.hasOwnProperty.call(collection, 'itemType')) {
          const links = collection.links
          const linksLength = links.length
          for (let i = 0; i < linksLength; i++) {
            if (links[i].rel.search(/coverage/i) > -1) {
              collection.itemType = 'coverage'
              break
            }
          }
        }
      })
      return collectionsJson.value.collections
    } else {
      return []
    }
  })

  return {
    collections,
    collectionsTotal,
    collectionsJson
  }
}