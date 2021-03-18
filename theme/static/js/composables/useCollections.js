import { ref, computed, onMounted } from 'https://cdnjs.cloudflare.com/ajax/libs/vue/3.0.7/vue.esm-browser.prod.js'

export default function useCollections() {
  const collectionsJson = ref({})
  const collectionsTotal = computed(() => {
    if (Object.prototype.hasOwnProperty.call(collectionsJson.value, 'numberMatched')) {
      return collectionsJson.value.numberMatched
    } else {
      return 0
    }
  })
  const collections = computed(() => {
    if (Object.prototype.hasOwnProperty.call(collectionsJson.value, 'collections')) {
      return collectionsJson.value.collections
    } else {
      return []
    }
  })
  const getCollections = async () => {
    try {
      const resp = await axios.get('?f=json') // relative to /collections
      collectionsJson.value = resp.data
    } catch (err) {
      console.error(err)
    }
  }
  
  onMounted(getCollections)

  return {
    collections,
    collectionsTotal,
    collectionsJson,
    getCollections
  }
}