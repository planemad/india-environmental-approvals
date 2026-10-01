## What is an environment clearance?

An environmental clearance (EC) is a permission from the government that a project in India needs **before it is built or expanded**. It is required under the [Environment Impact Assessment (EIA) Notification, 2006](https://environmentclearance.nic.in/writereaddata/EIA_notifications/2006_09_14_EIA.pdf), for projects that can harm the environment, such as mines, factories, power plants, large buildings, highways and dams. The idea is that the effects on air, water, land, forests and nearby people are studied and made public before work starts, and the permission comes with conditions the project must follow.

The dashboard tracks applications on [PARIVESH](https://parivesh.nic.in), the government's single online portal for these permissions.

## Who decides?

Projects are sorted into categories by size and likely impact.

| Category | Typical projects | Reviewed by | Decided by |
|---|---|---|---|
| **A** | Large mines, power plants, big industries | Expert Appraisal Committee (EAC), central | Union environment ministry (MoEFCC) |
| **B1** | Medium projects | State Expert Appraisal Committee (SEAC) | State Environment Impact Assessment Authority (SEIAA) |
| **B2** | Smaller projects, most small mines and buildings | SEAC | SEIAA, without a full EIA study |

## The steps of an EC application

1. **Apply online.** The project proponent fills in the application form and uploads documents on PARIVESH.
2. **Screening.** For Category B projects, the state committee decides whether a full EIA study is needed (B1) or not (B2).
3. **Scoping and Terms of Reference (ToR).** For projects that need an EIA, the committee fixes what the study must cover. The ToR is **not** a clearance. It only says what to study.
4. **EIA study and public consultation.** The applicant carries out the study and, for most projects, holds a public hearing where local people can comment.
5. **Final application and appraisal.** The EIA report is submitted and the expert committee reviews it. At this stage the reviewers often ask for more information (EDS: Essential Details Sought, or ADS: Additional Details Sought), and the proposal waits until the applicant replies.
6. **Decision.** The committee recommends and the authority (MoEFCC or SEIAA) either **grants** the EC with conditions or **rejects** it.
7. **After the decision.** The holder must follow the conditions and file compliance reports. Later they can apply to transfer, amend, extend or surrender the clearance.

## Other clearances you will see in the data

PARIVESH also handles permissions that are not ECs, and they follow their own steps.

- **Forest clearance (FC):** permission to use forest land for something other than forestry. It usually has two stages. *Stage-I* is an in-principle approval. After the applicant pays compensation and meets the conditions, *Stage-II* or a *Final Diversion Order* gives the final permission.
- **Wildlife clearance:** for projects in or near a protected area.
- **CRZ clearance:** for projects on the coast, under the Coastal Regulation Zone rules.
- **Follow-up applications:** transfers, amendments, validity extensions and corrigenda on an existing clearance.

## How the dashboard groups the stages

The portal uses well over a hundred different status labels. The dashboard groups them into the outcomes below. The grouping is defined in [`pipeline.json`](https://github.com/publicmap/parivesh-dashboard/blob/main/pipeline.json), and each status is explained in the [glossary](https://github.com/publicmap/parivesh-dashboard/blob/main/glossary.md).

| Group | What it means |
|---|---|
| **Granted** | A final clearance has been given: EC, CRZ clearance, Stage-II forest clearance, final diversion order or approval. |
| **ToR granted** | Terms of Reference were issued. The study can begin, but the EC itself has not been decided. |
| **Pending** | The application is still moving: waiting for verification, under review, waiting for the applicant's reply, or waiting for payments and compliance. |
| **Rejected** | The authority turned the application down or later cancelled it. |
| **Withdrawn or returned** | The applicant pulled it back, or it was sent back as unacceptable. No decision was made on its merits. |
| **Delisted** | The portal removed it, usually because the application was left incomplete. These are left out of the application counts. |

The small progress bar at the start of each row in the project list shows how far a pending project has come, in five steps: **submitted, scrutiny, appraisal, decision and compliance, outcome**. Its colour shows the group.

## Things to keep in mind

- **A grant date does not always mean a clearance was granted.** Some withdrawn or pending proposals also carry a grant date, so the dashboard groups by status instead.
- **Pending is not the same as slow.** A proposal may be waiting on the applicant, not the government.
- **The labels come from the portal.** PARIVESH does not publish official definitions, so the explanations here are compiled from the names of the workflow stages and the regulations. Treat them as explanatory, not legal text.
- For the official record of any project, use the **proposal link** in the project list to open it on PARIVESH.
