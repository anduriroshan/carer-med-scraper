import asyncio
import merging_collections
from crawl_page.crawl_page_gastroenterology import (
    fetch_new_gut_articles,
    fetch_new_Gastroenterology_articles,
    fetch_heptology_articles,
    fetch_new_ajg_articles,
    fetch_new_clinical_gas_hept_articles,
)
from crawl_page.crawl_page_diabetes import (
    fetch_new_diabetes_articles,
    fetch_new_endocrinology_articles,
    fetch_new_diabetes_care_articles,
    fetch_new_diabetologia_articles,
    fetch_new_endocrine_review_articles,
)
from crawl_page.crawl_page_cardiology import (
    fetch_new_european_heart_journal_articles,
    fetch_new_academic_oup_articles,
    fetch_new_heart_bmj_articles,
    fetch_new_circulation_articles,
    fetch_new_jacc_articles,
)
from crawl_page.crawl_page_clinical_medicine import (
    crawl_page_lancet,
    crawl_page_jama,
    crawl_page_nejm,
    crawl_page_bmj,
    crawl_page_aoim
)
from crawl_page.crawl_page_neuroscience import (
    crawl_page_nature,
    crawl_page_neuron,
    crawl_page_brain,
    crawl_page_trends,
    crawl_page_jneuro
)

from crawl_page.crawl_page_dermatology import (
    crawl_page_jaad,
    crawl_page_jama_derma,
    crawl_page_jid,
    crawl_page_derma_clinics
)

from crawl_page.crawl_page_immunology import (
    crawl_page_natural,
    crawl_page_immunity,
    crawl_page_immu_trends,
    crawl_page_jaci,
    crawl_page_annual
)
from crawl_page.crawl_page_oncology import (
    crawl_page_cancer_cell,
    crawl_page_ascjournal,
    crawl_page_lancet_onco,
    crawl_page_nature_cancer,
    crawl_page_jco
)

from crawl_page.crawl_page_pediatrics import (
    crawl_page_jama_ped,
    crawl_page_pediatrics,
    crawl_page_adc_bmj,
    crawl_page_jpeds,
    crawl_page_nature_ped
)

from crawl_page.crawl_page_ophthalmology import (
    crawl_page_jama_ophtha,
    crawl_page_bjo_bmj,
)

from crawl_page.crawl_page_nephrology  import(
    crawl_page_ajkd,
    crawl_page_cjasn,
    crawl_page_jasn,
    crawl_page_kidney_international,
    crawl_article_ndt
)

from crawl_page.crawl_page_pulmonology import (
    crawl_page_ajrccm,
    crawl_page_chest,
    crawl_page_erj,
    crawl_page_respiratory,
    crawl_page_thorax
)

from crawl_page.crawl_page_rheumatology import (
    crawl_page_rheumatology,
    crawl_page_ard,
    crawl_page_art,
    crawl_page_arthritis,
    crawl_page_clincal_r
)

from crawl_page.crawl_page_infectious import (
    crawl_page__journal_id,
    crawl_page_cdc,
    crawl_page_clinical_id,
    crawl_page_infection,
    crawl_page_lancet_id
)

from crawl_page.crawl_page_hematology import (
    crawl_page_blood,
    crawl_page_blood_advances,
    crawl_page_haematologica,
    crawl_page_leukemia
)

from crawl_page.crawl_page_obstetrics_gyno import(
    crawl_page_ajog,
    crawl_page_bjog,
    crawl_page_fertstert,
    crawl_page_obs_gyn
)

from crawl_page.crawl_page_orthopedics import (
    crawl_page_bone,
    crawl_page_clinical_ortho,
    crawl_page_jbjs,
    crawl_page_jortho,
    crawl_page_jorunal_orthopaedic
)

from crawl_page.crawl_page_urology import(
    crawl_page_urology,
    crawl_page_aua,
    crawl_page_european_urology,
    crawl_page_bjui,
    crawl_page_world_urology,

)

from crawl_page.crawl_page_otolaryngology import (
    crawl_page_ear_hearing,
    crawl_page_jama_ent,
    crawl_page_laryngoscope,
    crawl_page_sage
)

from crawl_page.crawl_page_radiology import (
    crawl_page_radiology,
    crawl_page_arj,
    crawl_page_european_radiology,
    crawl_page_investigative_radiology,
    crawl_page_magnetic_resonance
)

from crawl_page.crawl_page_anesthesiology import(
    crawl_page_analgesia,
    crawl_page_anesthesiology,
    crawl_page_bja,
    crawl_page_clinical_anesthesia,
    crawl_page_rapm
)

from crawl_page.crawl_page_pathology import (
    crawl_page_ajp,
    crawl_page_histopathology,
    crawl_page_journal_pathology,
    crawl_page_springer,
)

from crawl_page.crawl_page_psychiatry import(
    crawl_page_ajp,
    crawl_page_jama_psy,
    crawl_page_molecular_psychiatry,
    crawl_page_schizophrenia
)

from crawl_page.crawl_page_geriatrics import (
    crawl_page_age,
    crawl_page_ags,
    crawl_page_gerontology,
    crawl_page_ggi
)

from crawl_page.crawl_page_allergy import (
    crawl_page_allergy,
    crawl_page_cea,
    crawl_page_co_allergy,
    crawl_page_iaa
)

from utils import setup_database



async def gasteroentrology(conn):
    await fetch_new_gut_articles(conn)
    await fetch_new_Gastroenterology_articles(conn)
    await fetch_heptology_articles(conn)
    await fetch_new_ajg_articles(conn)
    await fetch_new_clinical_gas_hept_articles(conn)
    print("=> Gastroenterology articles updated successfully")

async def diabetes_and_endocrinology(conn):
    await fetch_new_diabetes_articles(conn)
    await fetch_new_endocrinology_articles(conn)
    await fetch_new_diabetes_care_articles(conn)
    await fetch_new_diabetologia_articles(conn)
    await fetch_new_endocrine_review_articles(conn)
    print("=> Diabetes and Endocrinology articles updated successfully")

async def cardiology(conn):
    await fetch_new_circulation_articles(conn)
    await fetch_new_european_heart_journal_articles(conn)
    await fetch_new_academic_oup_articles(conn)
    await fetch_new_heart_bmj_articles(conn)
    await fetch_new_jacc_articles(conn)
    print("=> Cardiology articles updated successfully")

async def clinical_medicine(conn):
    await crawl_page_lancet(conn)
    await crawl_page_jama(conn)
    await crawl_page_nejm(conn)
    await crawl_page_bmj(conn)
    await crawl_page_aoim(conn)
    print("=> Clinical Medicine articles updated successfully")

async def neuroscience(conn):
    await crawl_page_nature(conn)
    await crawl_page_neuron(conn)
    await crawl_page_brain(conn)
    await crawl_page_trends(conn)
    await crawl_page_jneuro(conn)
    print("=> Neuroscience articles updated successfully")

async def dermatology(conn):
    await crawl_page_jaad(conn)
    await crawl_page_jama_derma(conn)
    await crawl_page_jid(conn)
    await crawl_page_derma_clinics(conn)
    print("=> Dermatology articles updated successfully")

async def immunology(conn):
    await crawl_page_natural(conn)
    await crawl_page_immunity(conn)
    await crawl_page_immu_trends(conn)
    await crawl_page_jaci(conn)
    await crawl_page_annual(conn)
    print("=> Immunology articles updated successfully")

async def oncology(conn):
    await crawl_page_cancer_cell(conn)
    await crawl_page_ascjournal(conn)
    await crawl_page_lancet_onco(conn)
    await crawl_page_nature_cancer(conn)
    await crawl_page_jco(conn)
    print("=> Oncology articles updated successfully")

async def pediatrics(conn):
    await crawl_page_pediatrics(conn)
    await crawl_page_adc_bmj(conn)
    await crawl_page_jpeds(conn)
    await crawl_page_nature_ped(conn)
    print("=> Pediatrics articles updated successfully")

async def opthalmology(conn):
    await crawl_page_jama_ophtha(conn)
    await crawl_page_bjo_bmj(conn)
    print("=> Ophthalmology articles updated successfully")

async def nephrology(conn):
    await crawl_page_ajkd(conn)
    await crawl_page_cjasn(conn)
    await crawl_page_jasn(conn)
    await crawl_page_kidney_international(conn)
    await crawl_article_ndt(conn)
    print("=> Nephrology articles updated successfully")

async def pulmonology(conn):
    await crawl_page_ajrccm(conn)
    await crawl_page_chest(conn)
    await crawl_page_erj(conn)
    await crawl_page_respiratory(conn)
    await crawl_page_thorax(conn)
    print("=> Pulmonology articles updated successfully")

async def rheumatology(conn):
    await crawl_page_rheumatology(conn)
    await crawl_page_ard(conn)
    await crawl_page_art(conn)
    await crawl_page_arthritis(conn)
    await crawl_page_clincal_r(conn)
    print("=> Rheumatology articles updated successfully")

async def infectious_diseases(conn):
    await crawl_page__journal_id(conn)
    await crawl_page_cdc(conn)
    await crawl_page_clinical_id(conn)
    await crawl_page_infection(conn)
    await crawl_page_lancet_id(conn)
    print("=> Infectious Diseases articles updated successfully")

async def hematology(conn):
    await crawl_page_blood(conn)
    await crawl_page_blood_advances(conn)
    await crawl_page_haematologica(conn)
    await crawl_page_leukemia(conn)
    print("=> Hematology articles updated successfully")

async def obstetrics_gynocology(conn):
    await crawl_page_ajog(conn)
    await crawl_page_bjog(conn)
    await crawl_page_fertstert(conn)
    await crawl_page_obs_gyn(conn)
    print("=> Obstetrics & Gynocology articles updated successfully")

async def orthopaedics(conn):
    await crawl_page_bone(conn)
    await crawl_page_clinical_ortho(conn)
    await crawl_page_jbjs(conn)
    await crawl_page_jortho(conn)
    await crawl_page_jorunal_orthopaedic(conn)
    print("=> Orthopaedics articles updated successfully")

async def urology(conn):
    await crawl_page_urology(conn)
    await crawl_page_aua(conn)
    await crawl_page_european_urology(conn)
    await crawl_page_bjui(conn)
    await crawl_page_world_urology(conn)
    print("=> Urology articles updated successfully")

async def otolaryngology(conn):
    await crawl_page_ear_hearing(conn)
    await crawl_page_jama_ent(conn)
    await crawl_page_laryngoscope(conn)
    await crawl_page_sage(conn)
    print("=> Otolaryngology (ENT) articles updated successfully")

async def radiology(conn):
    await crawl_page_radiology(conn)
    await crawl_page_arj(conn)
    await crawl_page_european_radiology(conn)
    await crawl_page_investigative_radiology(conn)
    await crawl_page_magnetic_resonance(conn)
    print("=> Radiology articles updated successfully")

async def anesthesiology(conn):
    await crawl_page_analgesia(conn)
    await crawl_page_anesthesiology(conn)
    await crawl_page_bja(conn)
    await crawl_page_clinical_anesthesia(conn)
    await crawl_page_rapm(conn)
    print("=> Anesthesiology articles updated successfully")

async def pathology(conn):
    await crawl_page_ajp(conn)
    await crawl_page_histopathology(conn)
    await crawl_page_journal_pathology(conn)
    await crawl_page_springer(conn)
    print("=> Pathology articles updated successfully")

async def psychiatry(conn):
    await crawl_page_ajp(conn)
    await crawl_page_jama_psy(conn)
    await crawl_page_molecular_psychiatry(conn)
    await crawl_page_schizophrenia(conn)
    print("=> Psychiatry articles updated successfully")

async def geriatrics(conn):
    await crawl_page_age(conn)
    await crawl_page_ags(conn)
    await crawl_page_gerontology(conn)
    await crawl_page_ggi(conn)
    print("=> Geriatrics articles updated successfully")

async def allergy_immunology(conn):
    await crawl_page_allergy(conn)
    await crawl_page_cea(conn)
    await crawl_page_co_allergy(conn)
    await crawl_page_iaa(conn)
    print("=> Allery & Immunology articles updated successfully")


async def main_series():
    with setup_database() as conn:
        if conn:
            for i in range(1):
                #await pulmonology(conn)
                await rheumatology(conn)
                #await infectious_diseases(conn)
                #await hematology(conn)
                #await obstetrics_gynocology(conn)
                #await orthopaedics(conn)
                #await urology(conn)
                #await otolaryngology(conn)
                #await radiology(conn)
                #await anesthesiology(conn)
                #await pathology(conn)
                #await psychiatry(conn)
                #await geriatrics(conn)
                #await allergy_immunology(conn)
                #await nephrology(conn)
                #await opthalmology(conn)
                #await pediatrics(conn)
                #await oncology(conn)
                #await immunology(conn)
                #await dermatology(conn)
                #await neuroscience(conn)
                #await clinical_medicine(conn)
                #await gasteroentrology(conn)
                #await diabetes_and_endocrinology(conn)
                #await cardiology(conn)
            # do this for 2 iterations.
            cursor = conn.cursor()
            cleanup_query = "DELETE FROM article_links WHERE article_link = ''"
            cursor.execute(cleanup_query)
            conn.commit()
            cursor.close()
            print("Cleaned empty strings from the table.")

        else:
            print("Database setup failed")

async def main():
    with setup_database() as conn:
        if conn:
            for i in range(2):
                await asyncio.gather(
                    pulmonology(conn),
                    rheumatology(conn),
                    infectious_diseases(conn),
                    hematology(conn),
                    obstetrics_gynocology(conn),
                    orthopaedics(conn),
                    urology(conn),
                    otolaryngology(conn),
                    radiology(conn),
                    anesthesiology(conn),
                    pathology(conn),
                    psychiatry(conn),
                    geriatrics(conn),
                    allergy_immunology(conn),
                    nephrology(conn),
                    opthalmology(conn),
                    pediatrics(conn),
                    oncology(conn),
                    immunology(conn),
                    dermatology(conn),
                    neuroscience(conn),
                    clinical_medicine(conn),
                    gasteroentrology(conn),
                    diabetes_and_endocrinology(conn),
                    cardiology(conn)
                )
            # do this for 2 iterations.
        # **Run SQL cleanup query after tasks complete**
            cursor = conn.cursor()
            cleanup_query = "DELETE FROM article_links WHERE article_link = ''"
            cursor.execute(cleanup_query)
            conn.commit()
            cursor.close()
            print("Cleaned empty strings from the table.")

        else:
            print("Database setup failed")
        merging_collections.main()

if __name__ == "__main__":
    asyncio.run(main())
 
