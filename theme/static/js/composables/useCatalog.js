import { ref, computed } from 'https://cdnjs.cloudflare.com/ajax/libs/vue/3.0.7/vue.esm-browser.prod.js'

export default function useCatalog() {
  const catalogJson = ref(JSON_DATA)
  const linksTotal = computed(() => {
    return links.value.length
  })
  const links = computed(() => {
    if (Object.prototype.hasOwnProperty.call(catalogJson.value, 'links')) {
      return catalogJson.value.links
    } else {
      return []
    }
  })
  const childLinks = computed(() => {
    let children = []
    links.value.forEach(link => {
      if ((link.rel === 'child' || link.rel === 'item') && link.type === 'text/html') {
        const splitLink = link.href.split('/')
        link.name = splitLink[splitLink.length-1]
        children.push(link)
      }
    })
    return children
  })

  return {
    links,
    childLinks,
    linksTotal,
    catalogJson
  }
}